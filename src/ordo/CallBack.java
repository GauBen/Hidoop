package ordo;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallBack extends Remote, Serializable {

    public void done() throws RemoteException, InterruptedException;

    ;

}
