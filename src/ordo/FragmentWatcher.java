package ordo;

import java.util.List;
import java.util.TimerTask;

public class FragmentWatcher extends TimerTask {

    /**
     * Si un fragment depasse RESTART_THRESHOLD * mean time d'un fragment, il est redemarre
     */
    final int RESTART_THRESHOLD = 3;

    @Override
    public void run() {
        FragmentsHandler fragmentsHandler = Job.fragmentsHandler;

        long averageTime = fragmentsHandler.meanExecutionTime();

        List<Integer> currentlyProcessing = fragmentsHandler.currentlyProcessingFragmentIds();

        for(int fragId : currentlyProcessing){
            if(fragmentsHandler.getExecutionTime(fragId) > RESTART_THRESHOLD * averageTime){
                // Restart the job on an other node
                // How ? Mark the fragment as unprocessed and see if any of the worker can take him
                // (any except the one that had it first)
            }
        }

    }
}
