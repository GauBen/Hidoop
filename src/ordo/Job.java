package ordo;


import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import map.MapReduce;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class Job implements JobInterfaceX, CallBack {
    // TODO

    /**
     * Settings for RMI
     */
    static String serverAddress = "//localhost";
    static int port = 4000;
    // MapReduce mapReduce;
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

    }

    @Override
    public void startJob(MapReduce mr) {
        // this.mapReduce = mr; TODO : determiner si il faut le garder en attribut.

        // Store RMI connections
        Worker[] nodes = new Worker[this.numberOfMaps];

        // Connect to all node
        for (int i = 0; i < this.numberOfMaps; i++) {
            try {
                nodes[i] = ((Worker) Naming.lookup(Job.serverAddress + ":" + Job.port + "/Node" + i));
            } catch (MalformedURLException | RemoteException | NotBoundException e) {
                e.printStackTrace();
            }
        }


        // Set the Format
        Format iFormat = this.getFormatFromType(this.inputFormat);
        Format oFormat = this.getFormatFromType(this.outputFormat);


        for (Worker node : nodes) {
            try {
                node.runMap(mr, iFormat, oFormat, this);
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }


        //TODO : le demarrer ? S'en occuper.
    }

    public Format getFormatFromType(Format.Type type) {
        Format format;
        switch (type) {
            case KV:
                format = new KVFormat(inputFname);
                break;
            case LINE:
                format = new LineFormat(inputFname);
                break;
            default:
                format = null;
        }
        if (format == null) {
            throw new RuntimeException("Invalid format " + type);
        }

        return format;
    }

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

    /**
     * Called by the nodes when they are done Mapping
     */
    @Override
    public void done() {
        this.numberOfMapsDone++;
    }

}
