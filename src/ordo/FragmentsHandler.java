package ordo;

import java.net.URI;
import java.util.*;

import hdfs.HdfsNameServer.FragmentInfo;

public class FragmentsHandler {

    static final int STATE_NOT_PROCESSED = 0;
    static final int STATE_IN_PROGRESS = 1;
    static final int STATE_PROCESSED = 2;

    /**
     * Integer : id du fragment List : toutes les FragmentInfo qui ont cet ID
     */
    private HashMap<URI, List<FragmentInfo>> allFragments = new HashMap<>();

    /**
     * Set of all URIs.
     */
    private Set<URI> allUri = new HashSet<>();

    /**
     * Stocke l'etat des fragments (Traite, en cours de traitement, disponible)
     */
    private HashMap<Integer, Integer> fragmentsStates = new HashMap<>();

    public FragmentsHandler(List<FragmentInfo> allFragments) {
        for (FragmentInfo info : allFragments) {
            int id = info.id;
            URI uri = info.node;

            this.allUri.add(uri);

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

    public URI getUriFromAddress(String ip, int port){
        for (URI uri : this.allUri){
            if(uri.getHost() == ip && uri.getPort() == port){
                return uri;
            }
        }
        System.out.println("Correspondance adresse - URI non trouvée !");
        return null;
    }

    public Set<URI> getAllWorkers() {
        return this.allFragments.keySet();
    }


}
