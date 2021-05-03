package ordo;

import hdfs.FragmentInfo;
import hdfs.HdfsNodeInfo;

import java.util.*;
import java.util.stream.Collectors;

public class FragmentsHandler {

    static final int STATE_NOT_PROCESSED = 0;
    static final int STATE_IN_PROGRESS = 1;
    static final int STATE_PROCESSED = 2;

    private final List<FragmentInfo> rawFragmentList;

    /**
     * Integer : id du fragment List : toutes les FragmentInfo qui ont cet ID La
     * string correspond à URI.toString
     */
    private final HashMap<String, List<FragmentInfo>> allFragments = new HashMap<>();

    /**
     * Stocke l'etat des fragments (Traite, en cours de traitement, disponible)
     */
    private final HashMap<Integer, Integer> fragmentsStates = new HashMap<>();

    /**
     * Stores the time it took to process a fragment
     */
    private final HashMap<Integer, Long> fragmentsTime = new HashMap<>();

    /**
     * Stores who is handling the fragment
     */
    private final HashMap<Integer, String> fragmentNodeUsed = new HashMap<>();

    public FragmentsHandler(List<FragmentInfo> allFragments) {
        this.rawFragmentList = allFragments;

        for (FragmentInfo info : allFragments) {
            int id = info.id;
            HdfsNodeInfo uri = info.node;

            if (!this.allFragments.containsKey(uri.toString())) {
                this.allFragments.put(uri.toString(), new ArrayList<>());
            }
            this.fragmentsStates.put(id, STATE_NOT_PROCESSED);
            this.allFragments.get(uri.toString()).add(info);
            this.fragmentNodeUsed.put(id, "No node assigned");
        }

        System.out.println("C'est parti! Il y a " + fragmentsStates.size() + " fragments uniques.");
    }

    /**
     * Called when a node is looking for a new job
     *
     * @param uri
     * @return FragmentInfo | null
     */
    public FragmentInfo getAvailableFragmentForURI(HdfsNodeInfo uri) {
        for (FragmentInfo info : this.allFragments.get(uri.toString())) {
            if (this.fragmentsStates.get(info.id) == STATE_NOT_PROCESSED) {
                this.fragmentsStates.put(info.id, STATE_IN_PROGRESS);
                this.fragmentsTime.put(info.id, System.currentTimeMillis());
                this.fragmentNodeUsed.put(info.id, uri.toString());
                return info;
            }
        }

        System.out.println("Pas de fragment supplementaire trouve pour " + uri);

        return null;
    }

    /**
     * Mark a fragment as done
     *
     * @param fragmentId
     */
    public void setFragmentDone(int fragmentId) {
        if (this.fragmentsTime.getOrDefault(fragmentId, ((long) -1)) == -1) {
            this.fragmentsTime.put(fragmentId, (long) -1);
        } else {
            this.fragmentsTime.put(fragmentId, System.currentTimeMillis() - this.fragmentsTime.get(fragmentId));
        }
        this.fragmentsStates.put(fragmentId, STATE_PROCESSED);
    }

    /**
     * Get mean execution time of finished fragments
     *
     * @return
     */
    public long meanExecutionTime() {
        long time = 0;
        for (int fragmentId : this.fragmentsTime.keySet()) {
            if (this.fragmentsStates.get(fragmentId) == STATE_PROCESSED) {
                if (this.fragmentsTime.get(fragmentId) > 0L) {
                    time += this.fragmentsTime.get(fragmentId);
                }
            }

        }
        return time / this.finishedFragments();
    }

    /**
     * Get a list of all the fragment currently processing
     *
     * @return
     */
    public List<Integer> currentlyProcessingFragmentIds() {
        List<Integer> list = new ArrayList<>();

        for (int fragmentId : this.fragmentsStates.keySet()) {
            if (this.fragmentsStates.get(fragmentId) == STATE_PROCESSED) {
                list.add(fragmentId);
            }
        }

        return list;
    }

    /**
     * Get execution time for a specific fragment ID
     *
     * @param fragmentId
     * @return
     */
    public long getExecutionTime(int fragmentId) {
        if (this.fragmentsStates.get(fragmentId) == STATE_IN_PROGRESS) {
            return System.currentTimeMillis() - this.fragmentsTime.get(fragmentId);
        } else {
            return this.fragmentsTime.get(fragmentId);
        }
    }

    /**
     * Used when a node fails to process a fragment to find an alternative node
     * @param id
     * @param nodeToAvoid
     * @return
     */
    public FragmentInfo getAlternativeForFragment(int id, String nodeToAvoid){
        for (FragmentInfo fragmentInfo : this.rawFragmentList){
            if(fragmentInfo.id == id && !fragmentInfo.node.toString().equals(nodeToAvoid)){
                System.out.println("Alternative trouvee pour le fragment " + id + " !");
                this.fragmentsStates.put(fragmentInfo.id, STATE_IN_PROGRESS);
                this.fragmentsTime.put(fragmentInfo.id, System.currentTimeMillis());
                this.fragmentNodeUsed.put(fragmentInfo.id, fragmentInfo.node.toString());
                return fragmentInfo;
            }
        }

        System.out.println("Pas d'alternatives trouvee pour le fragment " + id + " !");
        return null;
    }

    /**
     * Who is processing the fragment
     * @param fragmentId
     * @return
     */
    public String getWorkerProcessingFragment(int fragmentId){
        return this.fragmentNodeUsed.get(fragmentId);
    }

    /**
     * Get the number of finished fragments
     * @return
     */
    public int finishedFragments() {
        int finished = 0;

        for(int state : this.fragmentsStates.values()){
            if(state == STATE_PROCESSED){
                finished++;
            }
        }

        return  finished;
    }

    /**
     * Get the URI of all Workers
     * @return
     */
    public Set<HdfsNodeInfo> getAllWorkers() {
        Set<HdfsNodeInfo> allWorkers = new HashSet<>();
        // Transform allFragments to a list
        for (FragmentInfo info : this.allFragments.values().stream().flatMap(List::stream)
                .collect(Collectors.toList())) {
            allWorkers.add(info.node);
        }

        return allWorkers;
    }

}
