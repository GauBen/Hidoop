package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {

    private static final long serialVersionUID = 1L;
    AtomicInteger numberOfTasksDone = new AtomicInteger(0);
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
        this.numberOfTasksDone.getAndAdd(1);
        System.out.println("Le node " + id + " a fini en " + processDuration + ". Il reste " + (this.numberOfMaps - this.numberOfTasksDone.get()));
        // Free
        if (this.numberOfTasksDone.get() == this.numberOfMaps) {
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
