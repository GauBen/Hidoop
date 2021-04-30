package ordo;

import java.util.HashMap;
import java.util.List;

public class TasksHandler {

    static final int STATE_NOT_PROCESSED = 0;
    static final int STATE_IN_PROGRESS = 1;
    static final int STATE_PROCESSED = 2;

    List<HidoopTask> taskList;


    /**
     * Stocke l'etat des fragments (Traite, en cours de traitement, disponible)
     */
    private final HashMap<Integer, Integer> tasksStates = new HashMap<>();

    /**
     * Stores the time it took to process a fragment
     */
    private final HashMap<Integer, Long> tasksTime = new HashMap<>();

    /**
     * Stores who is handling the task
     */
    private final HashMap<Integer, String> taskNodeUsed = new HashMap<>();

    public TasksHandler(List<HidoopTask> taskList) {
        this.taskList = taskList;

        for (HidoopTask task : taskList){
            int id = taskList.indexOf(task);
            tasksStates.put(id, STATE_NOT_PROCESSED);
        }
    }

    public HidoopTask getAvailableTask(){
        for (HidoopTask task : taskList){
            int id = taskList.indexOf(task);
            if(this.tasksStates.get(id) == STATE_NOT_PROCESSED){
                this.tasksStates.put(id, STATE_IN_PROGRESS);
                return task;
            }
        }

        System.out.println("No more tasks available!");
        return null;
    }

    public void setTaskDone(int taskId){
        //this.tasksTime.put(taskId, System.currentTimeMillis() - this.tasksTime.get(fragmentId));
        this.tasksStates.put(taskId, STATE_PROCESSED);
    }

    public int getNumberOfTasksDone(){
        int done = 0;

        for (HidoopTask task : taskList){
            int id = this.taskList.indexOf(task);
            if(this.tasksStates.get(id) == STATE_PROCESSED){
                done++;
            }
        }
        return done;
    }




}
