package ordo;

import hdfs.HdfsNodeInfo;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallBack extends Remote, Serializable {

    void done(HdfsNodeInfo workerUri, long processDuration, int fragmentID) throws RemoteException, InterruptedException;

    void error(String nodeID, String texte) throws RemoteException, InterruptedException;

}
