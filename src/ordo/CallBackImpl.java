package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {

    private static final long serialVersionUID = 1L;
    int numberOfTasksDone = 0;
    int numberOfMaps;
    private Semaphore semaphore;

    public CallBackImpl(int numberOfMaps) throws RemoteException {
        this.numberOfMaps = numberOfMaps;
        this.semaphore = new Semaphore(0);
    }

    /**
     * Called by the nodes when they are done Mapping
     */
    @Override
    public void done(String id, long processDuration) throws RemoteException, InterruptedException {
        this.numberOfTasksDone++;
        System.out.println("Le node " + id + " a fini en " + processDuration);
        // Free
        if (this.numberOfTasksDone == this.numberOfMaps) {
            this.semaphore.release();
        }

    }

    @Override
    public void error(String nodeID, String texte) {
        System.out.println("Erreur du node " + nodeID + "\n" + texte);
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }
}
