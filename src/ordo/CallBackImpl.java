package ordo;

import hdfs.HdfsNodeInfo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {

    private static final long serialVersionUID = 1L;
    AtomicInteger numberOfTasksDone = new AtomicInteger(0);
    int numberOfMaps;
    private static final Semaphore semaphore = new Semaphore(1);

    public CallBackImpl(int numberOfMaps) throws RemoteException {
        this.numberOfMaps = numberOfMaps;
    }

    /**
     * Called by the nodes when they are done Mapping
     */
    @Override
    public void done(HdfsNodeInfo workerUri, long processDuration, int fragmentID) throws RemoteException, InterruptedException {
        semaphore.acquire();
        if(Job.job.isFileless()){
            Job.job.getTasksHandler().setTaskDone(fragmentID);
            System.out.println("> Le node " + workerUri + " a fini l'element " + fragmentID + " en " + processDuration + "ms. Il reste " + (this.numberOfMaps - Job.job.getTasksHandler().getNumberOfTasksDone()));
            if (Job.job.getTasksHandler().getNumberOfTasksDone() == numberOfMaps) {
                Job.job.allWorkersAreDone();
            } else {
                // Si il reste des fragments à traiter
                Job.job.attributeNewTaskTo(workerUri, this);
            }
        }else {
            System.out.println("> Le node " + workerUri + " a fini l'element " + fragmentID + " en " + processDuration + "ms. Il reste " + (this.numberOfMaps - Job.fragmentsHandler.finishedFragments()));
            Job.fragmentsHandler.setFragmentDone(fragmentID);

            // Free
            if (Job.fragmentsHandler.finishedFragments() == numberOfMaps) {
                Job.job.allWorkersAreDone();
            } else {
                // Si il reste des fragments à traiter
                Job.job.attributeNewWorkTo(workerUri, this);
            }
        }
        semaphore.release();
    }

    @Override
    public void error(String nodeID, String texte) {
        System.out.println("> /!\\ Erreur du node " + nodeID + "\n" + texte);
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }
}
