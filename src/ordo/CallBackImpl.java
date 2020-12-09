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
    public void done() throws RemoteException, InterruptedException {
        this.numberOfTasksDone++;

        // Free
        if (this.numberOfTasksDone == this.numberOfMaps) {
            this.semaphore.release();
        }

    }

    public Semaphore getSemaphore() {
        return semaphore;
    }
}
