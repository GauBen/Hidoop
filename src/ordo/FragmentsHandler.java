package ordo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import hdfs.FragmentInfo;
import hdfs.HdfsNodeInfo;

public class FragmentsHandler {

    static final int STATE_NOT_PROCESSED = 0;
    static final int STATE_IN_PROGRESS = 1;
    static final int STATE_PROCESSED = 2;

    /**
     * Integer : id du fragment List : toutes les FragmentInfo qui ont cet ID La
     * string correspond à URI.toString
     */
    private final HashMap<String, List<FragmentInfo>> allFragments = new HashMap<>();

    /**
     * Stocke l'etat des fragments (Traite, en cours de traitement, disponible)
     */
    private final HashMap<Integer, Integer> fragmentsStates = new HashMap<>();

    public FragmentsHandler(List<FragmentInfo> allFragments) {
        for (FragmentInfo info : allFragments) {
            int id = info.id;
            HdfsNodeInfo uri = info.node;

            if (!this.allFragments.containsKey(uri.toString())) {
                this.allFragments.put(uri.toString(), new ArrayList<>());
            }
            this.fragmentsStates.put(id, STATE_NOT_PROCESSED);
            this.allFragments.get(uri.toString()).add(info);

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
                return info;
            }
        }

        System.out.println("Pas de fragment supplementaire trouve pour " + uri);

        return null;
    }

    /**
     * Get the URI of all Workers
     *
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
