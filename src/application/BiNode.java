package application;

import hdfs.HdfsNode;
import ordo.Worker;
import ordo.WorkerImpl;

import java.rmi.RemoteException;

public class BiNode {

    public BiNode(String nameserverHost, int nameserverPort, String nodeRoot, String rmiHost, int rmiPort) throws RemoteException {

        HdfsNode hdfsNode = new HdfsNode(nameserverHost, nameserverPort, nodeRoot);


        Worker hidoopNode = new WorkerImpl(rmiHost, rmiPort, hdfsNode.getUrl(), hdfsNode.getServer().getPort());


    }

    public static void main(String[] args) {

    }
}
