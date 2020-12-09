package ordo;


import formats.Format;
import formats.Format.OpenMode;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import map.MapReduce;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class Job implements JobInterfaceX {
    // TODO

    /**
     * Settings for RMI
     */
    static String serverAddress = "//localhost";
    static int port = 4000;
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
        //TODO : changer cela pour que ce soit dynamique
        //  this.inputFormat = Format.Type.KV;
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

        // Store RMI connections
        Worker[] nodes = new Worker[this.numberOfMaps];

        // Connect to all node
        // TODO : ATTENTION AUX NOMS DES NODES
        for (int i = 0; i < this.numberOfMaps; i++) {
            try {
                nodes[i] = ((Worker) Naming.lookup(Job.serverAddress + ":" + Job.port + "/Node" + i));
            } catch (MalformedURLException | RemoteException | NotBoundException e) {
                e.printStackTrace();
            }
        }


        // Set the Format
        Format iFormat = this.getFormatFromType(this.inputFormat, HdfsClient.getFragmentName(inputFname));
        Format oFormat;

        // Create temp result file
        this.createTempFile();

        CallBackImpl callBack = null;
        try {
            callBack = new CallBackImpl(this.getNumberOfMaps());
        } catch (RemoteException e) {
            e.printStackTrace();
            return;
        }

        int n = 0;
        for (Worker node : nodes) {
            try {
                oFormat = this.getFormatFromType(this.outputFormat, HdfsClient.getFragmentName(outputFname));
                node.runMap(mr, iFormat, oFormat, callBack);

                n++;
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }


        try {
            callBack.getSemaphore().acquire();

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

        try {
            System.out.println("Creating a temporary file... " + getTempFolderPath() + this.getTempFileName());
            File file = new File(getTempFolderPath() + this.getTempFileName());
            file.createNewFile();
            // use HDFS to create a file
            Format format = this.getFormatFromType(this.outputFormat, getTempFolderPath() + this.getTempFileName());

            // Create the local file
            format.open(OpenMode.W);
            format.write(new KV());


            HdfsClient.HdfsWrite(outputFormat, getTempFolderPath() + this.getTempFileName(), 1); //TODO : verifier ce qu'est le repfactor

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public String getTempFileName() {
        return this.outputFname + "_temp";
    }

    public void doReduceJob() {
        // Get the complete file from the HDFS and replace the empty file
        // TODO : verifier que ça remplace bien
        HdfsClient.HdfsRead(this.getTempFileName(), getTempFolderPath() + this.getTempFileName());

        // We open the temp file
        Format iFormat = this.getFormatFromType(this.outputFormat, getTempFolderPath() + this.getTempFileName()); // TODO : remplacer par le nom du temp

        // We create the result file
        Format oFormat = this.getFormatFromType(this.outputFormat, getResFolderPath() + this.outputFname);

        // Do the reduce
        this.mapReduce.reduce(iFormat, oFormat);
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
        //TODO : changer mieux
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
