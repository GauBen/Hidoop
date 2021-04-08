package ordo;

import formats.Format;
import formats.Format.OpenMode;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import hdfs.HdfsNameServer.FragmentInfo;
import hdfs.HdfsNodeInfo;
import map.MapReduce;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Job implements JobInterfaceX {
    // TODO

    /**
     * Settings for RMI
     */
    static String rmiServerAddress = "127.0.0.1";
    static int rmiPort = 4000;
    MapReduce mapReduce;
    Format.Type inputFormat;
    String inputFname;
    int numberOfReduces;
    int numberOfMaps;
    Format.Type outputFormat;
    String outputFname;
    SortComparator sortComparator;

    FragmentsHandler fragmentsHandler;

    public static Job job;


    public Job() {
        super();
        // Ces valeurs sont écrasées plus tard
        this.numberOfMaps = 1;
        this.numberOfReduces = 1;

        this.outputFormat = Format.Type.KV;

        Job.job = this;
    }

    /**
     * Path du dossier qui contient les resultats temporaires
     *
     * @return String
     */
    public static String getTempFolderPath() {
        return System.getProperty("user.dir") + "/tmp/";
    }

    /**
     * Path du dossier qui contient lres resultats finaux
     *
     * @return String
     */
    public static String getResFolderPath() {
        return System.getProperty("user.dir") + "/res/";
    }

    @Override
    public void startJob(MapReduce mr) {
        this.mapReduce = mr;

        List<List<FragmentInfo>> fragmentsTable = HdfsClient.listFragments(this.inputFname);

        // Get all fragments
        // On transforme les fragments sous forme de liste
        List<FragmentInfo> fragments = Objects.requireNonNull(fragmentsTable)
                .stream().flatMap(List::stream).collect(Collectors.toList()); // TODO : Verifier

        this.fragmentsHandler = new FragmentsHandler(fragments);

        if (fragments == null) {
            System.out.println("Le fichier n'a pas ete trouve dans le HDFS!");
            System.exit(11);
        }

        this.numberOfMaps = fragmentsTable.size(); // One fragment = one runmap

        // Define the callback used to know when a worker is done
        CallBackImpl callBack;
        try {
            callBack = new CallBackImpl(this.getNumberOfMaps());
        } catch (RemoteException e) {
            e.printStackTrace();
            return;
        }


        for (HdfsNodeInfo workerUri : fragmentsHandler.getAllWorkers()) {
            Worker worker = Objects.requireNonNull(this.getWorkerFromUri(workerUri));
            try {
                for (int i = 0; i < worker.getNumberOfCores(); i++) {
                    FragmentInfo info = this.fragmentsHandler.getAvailableFragmentForURI(workerUri);

                    if (info != null) {
                        this.executeWork(worker, info, callBack);
                    }

                }
            } catch (RemoteException e) {
                System.out.println("Impossible de recuperer  le nombre de coeurs du worker! ");
            }
        }

    }

    public void attributeNewWorkTo(HdfsNodeInfo workerUri, CallBack callBack) {
        FragmentInfo fragment = this.fragmentsHandler.getAvailableFragmentForURI(workerUri);

        Worker worker = getWorkerFromUri(workerUri);

        if (fragment != null && worker != null) {
            // On demarre le traitement du fragment sur le node associe
            // TODO: si le fragment ne démarre pas bien, il faut marquer le fragment comme "NON TRAITE" au lieu de "EN COURS"

            this.executeWork(worker, fragment, callBack);

        }
    }

    private void executeWork(Worker worker, FragmentInfo info, CallBack callBack) {

        // Set the Format
        Format iFormat = this.getFormatFromType(this.inputFormat, info.getAbsolutePath());

        // TODO : tester quand la méthode sera définie
        FragmentInfo fragmentDuResultat = new FragmentInfo(getTempFileName(), info.id, info.lastPart,
                info.node, info.root);

        Format oFormat = this.getFormatFromType(this.outputFormat, fragmentDuResultat.getAbsolutePath());

        try {
            worker.runMap(this.mapReduce, iFormat, oFormat, callBack);
        } catch (RemoteException e) {
            System.out.println("Impossible de demarrer le runMap sur le worker ! ");
        }


    }


    private Worker getWorkerFromUri(HdfsNodeInfo workerUri) {
        String address = WorkerImpl.workerAddress(Job.rmiServerAddress, Job.rmiPort,
                workerUri.getHost(), workerUri.getPort());
        try {
            return (Worker) Naming.lookup(address.replace("hdfs://", ""));
        } catch (MalformedURLException | NotBoundException e) {
            e.printStackTrace();
            System.out.println("> Le node " + workerUri.toString() + " n'a pas ete trouve dans le registry");
        } catch (RemoteException e) {
            e.printStackTrace();
            System.out.println(
                    "> Le rmi registry n'est pas disponible sur " + Job.rmiServerAddress + ":" + Job.rmiPort);
        }
        return null;
    }


    /**
     * Called by Callback when all workers are done
     */
    public void allWorkersAreDone() {
        System.out.println("> All done ! Let's request a file refresh...");
        HdfsClient.requestRefresh(); // Trigger pour detecter le fichier de resultat temporaire qui a ete creer

        // When callback frees semaphores, all nodes are done
        this.doReduceJob();
    }

    /**
     * Declare the partial result file in HDFS
     */
    public void createTempFile() {

        // use HDFS to create a file
        Format format = this.getFormatFromType(this.outputFormat, getTempFolderPath() + this.getTempFileName());

        // Create the local file
        format.open(OpenMode.W);

        HdfsClient.HdfsWrite(outputFormat, getTempFolderPath() + this.getTempFileName(), 1);

    }

    public String getTempFileName() {
        return this.outputFname + "_result_temp";
    }

    /**
     * Appelée quand tous les workers on travaille sur leur fragment
     */
    public void doReduceJob() {

        System.out.println("> Let's reduce " + getTempFolderPath() + this.getTempFileName());

        // Create tmp folder if not existing
        Path path = Paths.get(getTempFolderPath());
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Get the complete file from the HDFS and replace the empty file
        System.out.println("> On telecharge les résultats des machines");
        // TODO : verifier que ça remplace bien
        HdfsClient.HdfsRead(this.getTempFileName(), getTempFolderPath() + this.getTempFileName());
        System.out.println("> Telechargement termine");
        // We open the temp file
        Format iFormat = this.getFormatFromType(this.outputFormat, getTempFolderPath() + this.getTempFileName());

        // Create res folder if not existing
        Path pathRes = Paths.get(getResFolderPath());
        try {
            Files.createDirectories(pathRes);
        } catch (IOException e) {
            e.printStackTrace();
        }


        // We create the result file
        File file = new File(getResFolderPath(), this.outputFname);
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(8);
        }
        Format oFormat = this.getFormatFromType(this.outputFormat, getResFolderPath() + this.outputFname);

        iFormat.open(OpenMode.R);
        oFormat.open(OpenMode.W);
        System.out.println("> On reduit les resulats en un seul...");
        // Do the reduce
        this.mapReduce.reduce(iFormat, oFormat);
        System.out.println("> Done ! Resultat dans " + getResFolderPath() + this.outputFname);

        // Delete temp file
        System.out.println("> Let's delete temporary files");
        HdfsClient.HdfsDelete(getTempFileName());
        // TODO : a tester quand ce sera implemente
    }

    public Format getFormatFromType(Format.Type type, String fName) {
        Format format;
        switch (type) {
            case KV:
                format = new KVFormat(fName);
                break;
            case LINE:
                format = new LineFormat(fName);
                break;
            default:
                format = null;
        }
        if (format == null) {
            throw new RuntimeException("Invalid format " + type);
        }

        return format;
    }

    /* Setters and getters */

    @Override
    public int getNumberOfReduces() {
        return this.numberOfReduces;
    }

    @Override
    public void setNumberOfReduces(int tasks) {
        this.numberOfReduces = tasks;
    }

    @Override
    public int getNumberOfMaps() {
        return this.numberOfMaps;
    }

    @Override
    public void setNumberOfMaps(int tasks) {
        this.numberOfMaps = tasks;
    }

    @Override
    public Format.Type getInputFormat() {
        return this.inputFormat;
    }

    @Override
    public void setInputFormat(Format.Type ft) {
        this.inputFormat = ft;
    }

    @Override
    public Format.Type getOutputFormat() {
        return this.outputFormat;
    }

    @Override
    public void setOutputFormat(Format.Type ft) {
        this.outputFormat = ft;
    }

    @Override
    public String getInputFname() {
        return this.inputFname;
    }

    @Override
    public void setInputFname(String fname) {
        this.inputFname = fname;
        // TODO : changer mieux
        this.outputFname = this.inputFname + "_result";
    }

    @Override
    public String getOutputFname() {
        return this.outputFname;
    }

    @Override
    public void setOutputFname(String fname) {
        this.outputFname = fname;
    }

    @Override
    public SortComparator getSortComparator() {
        return this.sortComparator;
    }

    @Override
    public void setSortComparator(SortComparator sc) {
        this.sortComparator = sc;
    }

}
