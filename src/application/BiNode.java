package application;

import hdfs.HdfsNode;
import ordo.WorkerImpl;

import java.net.URISyntaxException;
import java.rmi.RemoteException;

public class BiNode {

    /**
     * @param nameserverHost
     * @param nameserverPort
     * @param nodeRoot       : Dossier parent du contenu du HDFS
     * @param rmiHost
     * @param rmiPort
     * @throws RemoteException
     */
    public BiNode(String nameserverHost, int nameserverPort, String nodeRoot, String rmiHost, int rmiPort) {

        HdfsNode hdfsNode = new HdfsNode(nameserverHost, nameserverPort, nodeRoot);

        // Separate thread for the Hidoop node
        Thread thread = new Thread() {
            public void run() {
                try {
                    new WorkerImpl(rmiHost, rmiPort, hdfsNode.getExternalHostname(),
                            hdfsNode.getServer().getLocalPort(), nodeRoot);
                } catch (RemoteException | URISyntaxException e) {
                    e.printStackTrace();
                    System.out.println("Echec du lancement du worker");
                }

            }

        };

        thread.start();
        hdfsNode.run();

    }

    public static void usage() {
        System.out.println(
                "Usage : BiNode <Nameserver address> <Nameserver port> <Node's root folder> <RMI address> <RMI port>");
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            usage();
            return;
        }

        int nameserverPort;
        int rmiPort;

        try {
            nameserverPort = Integer.parseInt(args[1]);
            rmiPort = Integer.parseInt(args[4]);
        } catch (NumberFormatException e) {
            usage();
            return;
        }

        String nameserverHost = args[0];
        String nodeRootFolder = args[2];
        String rmiHost = args[3];

        new BiNode(nameserverHost, nameserverPort, nodeRootFolder, rmiHost, rmiPort);
    }
}
