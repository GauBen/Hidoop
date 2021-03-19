package ordo;

import hdfs.HdfsNameServer.FragmentInfo;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class FragmentsHandler {

    static final int STATE_NOT_PROCESSED = 0;
    static final int STATE_IN_PROGRESS = 1;
    static final int STATE_PROCESSED = 2;

    /**
     * Integer : id du fragment List : toutes les FragmentInfo qui ont cet ID
     */
    private final HashMap<URI, List<FragmentInfo>> allFragments = new HashMap<>();

    /**
     * Stocke l'etat des fragments (Traite, en cours de traitement, disponible)
     */
    private final HashMap<Integer, Integer> fragmentsStates = new HashMap<>();

    public FragmentsHandler(List<FragmentInfo> allFragments) {
        for (FragmentInfo info : allFragments) {
            int id = info.id;
            URI uri = info.node;

            if (this.allFragments.get(uri) == null) {
                this.allFragments.put(uri, new ArrayList<>());
                this.fragmentsStates.put(id, STATE_NOT_PROCESSED);
            }
            this.allFragments.get(uri).add(info);

        }
    }

    /**
     * Called when a node is looking for a new job
     *
     * @param uri
     * @return FragmentInfo | null
     */
    public FragmentInfo getAvailableFragmentForURI(URI uri) {
        for (FragmentInfo info : this.allFragments.get(uri)) {
            if (this.fragmentsStates.get(info.id) == STATE_NOT_PROCESSED) {
                this.fragmentsStates.put(info.id, STATE_IN_PROGRESS);
                return info;
            }
        }

        return null;
    }

    public Set<URI> getAllWorkers() {
        return this.allFragments.keySet();
    }


}
