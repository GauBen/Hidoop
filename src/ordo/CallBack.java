package ordo;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallBack extends Remote, Serializable {

    public void done(String id, long processDuration) throws RemoteException, InterruptedException;

    public void error(String nodeID, String texte) throws RemoteException, InterruptedException;

}
