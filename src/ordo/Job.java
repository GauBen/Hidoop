package ordo;

import formats.Format;
import formats.Format.OpenMode;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import hdfs.HdfsNameServer.FragmentInfo;
import map.MapReduce;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;

public class Job implements JobInterfaceX {
    // TODO

    /**
     * Settings for RMI
     */
    static String rmiServerAddress = "//localhost";
    static int rmiPort = 4000;
    MapReduce mapReduce;
    Format.Type inputFormat;
    String inputFname;
    // TODO : les numberOfReduces/Maps à determnier, donnés par HDFS ??
    int numberOfReduces;
    int numberOfMaps;
    Format.Type outputFormat;
    String outputFname;
    SortComparator sortComparator;
    private int numberOfMapsDone = 0;

    public Job() {
        super();
        // Empty
        this.numberOfMaps = 1;
        this.numberOfReduces = 1;
        // TODO : changer cela pour que ce soit dynamique
        // this.inputFormat = Format.Type.KV;
        this.outputFormat = Format.Type.KV;
    }

    public static String getTempFolderPath() {
        return System.getProperty("user.dir") + "/tmp/";
    }

    public static String getResFolderPath() {
        return System.getProperty("user.dir") + "/res/";
    }

    @Override
    public void startJob(MapReduce mr) {
        this.mapReduce = mr;

        // Get all fragments

        List<FragmentInfo> fragments = HdfsClient.listFragments(this.inputFname); // TODO : fix sur intellij

        this.numberOfMaps = fragments.size(); // One fragment = one runmap

        // Define the callback used to know when a worker is done
        CallBackImpl callBack = null;
        try {
            callBack = new CallBackImpl(this.getNumberOfMaps());
        } catch (RemoteException e) {
            e.printStackTrace();
            return;
        }

        // Connect to all node
        // TODO : ATTENTION AUX NOMS DES NODES
        int n = 0;
        for (FragmentInfo fragment : fragments) {

            URI addresseDuFragment = fragment.node;

            try {
                // Get the worker associated with the HDFS (and thus with the fragment)
                Worker worker = ((Worker) Naming.lookup(Job.rmiServerAddress + ":" + Job.rmiPort + "/Node" + "("
                        + addresseDuFragment.getHost() + " : " + addresseDuFragment.getPort() + ")"));

                // Set the Format
                Format iFormat = this.getFormatFromType(this.inputFormat,
                        fragment.getAbsolutePath() + fragment.getFragmentName());

                // TODO : tester quand la méthode sera définie
                FragmentInfo fragmentDuResultat = new FragmentInfo(getTempFileName(), fragment.id, fragment.lastPart,
                        fragment.node, fragment.root);

                Format oFormat = this.getFormatFromType(this.outputFormat,
                        fragment.getAbsolutePath() + fragmentDuResultat.getFragmentName());

                worker.runMap(mr, iFormat, oFormat, callBack);

                n++;

            } catch (NotBoundException e) {
                System.out.println("Le node " + addresseDuFragment + " n'a pas ete trouve dans le registry");
                return;
            } catch (MalformedURLException | RemoteException e) {
                System.out.println("Le rmi registry n'est pas disponible sur " + Job.rmiPort + ":" + Job.rmiPort);
                return;
            }
        }

        try {
            // We wait for all nodes to call CallBack
            callBack.getSemaphore().acquire();

            HdfsClient.requestRefresh(); // Trigger pour detecter le fichier de resultat temporaire qui a ete fait

            // When callback frees semaphores, all nodes are done
            this.doReduceJob();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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

    public void doReduceJob() {

        System.out.println("Let's reduce " + getTempFolderPath() + this.getTempFileName());

        // Get the complete file from the HDFS and replace the empty file
        // TODO : verifier que ça remplace bien
        HdfsClient.HdfsRead(this.getTempFileName(), getTempFolderPath() + this.getTempFileName());

        // We open the temp file
        Format iFormat = this.getFormatFromType(this.outputFormat, getTempFolderPath() + this.getTempFileName());

        // We create the result file
        File file = new File(getResFolderPath() + this.outputFname);
        try {
            file.createNewFile();

        } catch (IOException e) {
            e.printStackTrace();
        }
        Format oFormat = this.getFormatFromType(this.outputFormat, getResFolderPath() + this.outputFname);

        iFormat.open(OpenMode.R);
        oFormat.open(OpenMode.W);

        // Do the reduce
        this.mapReduce.reduce(iFormat, oFormat);

        // Delete temp file
        System.out.println("Let's delete temporary files");
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
