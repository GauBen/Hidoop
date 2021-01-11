/**
 * HDFS - Hidoop Distributed File System.
 *
 * Serveur développé par Théo Petit et Gautier Ben Aïm.
 */

package hdfs;

import formats.Format;
import formats.Format.OpenMode;
import formats.KV;
import formats.KVFormatS;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * Serveur HDFS, capable d'initier des opérations distribuées de lecture,
 * d'écriture et de suppression sur les noeuds.
 */
public class HdfsNameServer {

    /**
     * Port par défaut du serveur.
     */
    final public static int DEFAULT_PORT = 51200;

    /**
     * Temps (en ms) entre deux pings.
     */
    final public static int PING_INTERVAL = 5000;

    /**
     * Nombre de lignes stockées dans le buffer.
     */
    final public static int BUFFER_SIZE = 128;

    /**
     * Les actions possibles dans HDFS.
     */
    public enum Action {
        /** Reconstition d'un fichier fragmenté. */
        READ,
        /** Sauvegarde d'un fichier fragmenté. */
        WRITE,
        /** Suppression d'un fichier fragmenté. */
        DELETE,
        /** Requête d'un nouveau noeud à initialiser. */
        NEW_NODE,
        /** Requête de vérification d'activité. */
        PING,
        /** Réponse de vérification d'activité. */
        PONG,
        /** Le ping provient d'un noeud inconnu. */
        UNKNOWN_NODE,
        /** On veut connaître la liste des fragments d'un fichier. */
        LIST_FRAGMENTS,
        /** On veut mettre à jour la liste des fichiers. */
        FORCE_RESCAN
    }

    /**
     * Serveur qui traite les requêtes HDFS.
     */
    private ServerSocket server;

    /**
     * Liste des noeuds.
     */
    private volatile List<URI> nodes = new ArrayList<>();

    /**
     * Liste des racines des noeuds.
     */
    private Map<URI, String> roots = new HashMap<>();

    /**
     * Liste des fichiers.
     */
    private volatile Map<String, Map<Integer, List<URI>>> files = new HashMap<>();

    /**
     * Initialise un noeud HDFS sur le port par défaut 51200.
     */
    public HdfsNameServer() {
        this(DEFAULT_PORT);
    }

    /**
     * Initialise un noeud HDFS
     */
    public HdfsNameServer(int port) {
        try {
            this.server = new ServerSocket(port);
            System.out.println();
            System.out.println("Initialisation :");
            System.out.println("* Serveur principal lancé sur le port " + this.server.getLocalPort());
            System.out.println("* Ctrl+C pour arrêter le serveur");
            System.out.println();
            this.runPinger();
            this.runListener();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Impossible de lancer le serveur, le port est peut-être occupé.");
        }
    }

    /**
     * Crée un thread qui vérifie que les noeuds sont actifs.
     */
    private void runPinger() {
        HdfsNameServer self = this;
        class Pinger implements Runnable {
            @Override
            public void run() {
                while (true) {
                    self.sendPing();
                    try {
                        Thread.sleep(PING_INTERVAL);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }
        new Thread(new Pinger()).start();
    }

    /**
     * Envoie un ping à tous les noeuds.
     */
    public void sendPing() {

        // Nombre de noeuds supprimés
        int removed = 0;

        for (URI uri : new ArrayList<>(this.nodes)) {

            try {

                Socket sock = new Socket(uri.getHost(), uri.getPort());
                sock.setSoTimeout(1000);
                ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream());

                // On envoie ping et on attend pong
                outputStream.writeObject(Action.PING);
                if (inputStream.readObject() != Action.PONG) {
                    throw new SocketException("Noeud déconnecté.");
                }

            } catch (IOException | ClassNotFoundException e) {

                System.out.println("Ping : noeud " + uri + " déconnecté...");

                this.removeNode(uri);
                removed++;

            }

        }

        // S'il y a plus d'un noeud supprimé on affiche la liste des fichiers
        if (removed > 0) {
            this.printFiles();
        }

    }

    /**
     * Supprime la référence à un noeud dans la liste des noeuds et des fichiers.
     *
     * @param uri Une adresse de la forme hdfs://adresse:port
     */
    private void removeNode(URI uri) {

        this.nodes.remove(uri);
        this.roots.remove(uri);

        for (Map<Integer, List<URI>> map : this.files.values()) {
            for (List<URI> list : map.values()) {
                list.remove(uri);
            }
        }

    }

    /**
     * Lance l'attente des requêtes entrantes.
     */
    private void runListener() {
        while (true) {
            try {
                // On attend une connexion au serveur HDFS
                Socket sock = this.server.accept();
                ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream());
                ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());

                // On traite la requête entrante
                this.handleRequest(sock, inputStream, outputStream);
            } catch (IOException | IllegalArgumentException e) {
                e.printStackTrace();
                System.out.println("Une connexion a échoué.");
            }
        }
    }

    /**
     * On traite les chaussettes ouvertes.
     *
     * @param sock         Socket connectée
     * @param inputStream  Flux d'objets entrants
     * @param outputStream Flux d'objets sortants
     */
    private void handleRequest(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        try {
            Action action = (Action) inputStream.readObject();

            // On filtre l'action demandée
            if (action == Action.PING) {
                this.handlePing(sock, inputStream, outputStream);
            } else if (action == Action.READ) {
                this.handleRead(sock, inputStream, outputStream);
            } else if (action == Action.WRITE) {
                this.handleWrite(sock, inputStream, outputStream);
            } else if (action == Action.DELETE) {
                this.handleDelete(sock, inputStream, outputStream);
            } else if (action == Action.NEW_NODE) {
                this.handleNewNode(sock, inputStream, outputStream);
            } else if (action == Action.LIST_FRAGMENTS) {
                this.handleListFragments(sock, inputStream, outputStream);
            } else if (action == Action.FORCE_RESCAN) {
                this.handleForceRescan(sock, inputStream, outputStream);
            } else {
                throw new IllegalArgumentException("Action invalide.");
            }

        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
            System.out.println("Données invalides, connexion annulée.");
        } catch (URISyntaxException e) {
            e.printStackTrace();
            System.out.println("Problème d'adresse d'un noeud");
        }
    }

    /**
     * Réceptionne un ping. Si le noeud est connu, rien ne change, sinon on demande
     * au noeud de s'initialiser.
     */
    private void handlePing(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws IOException, ClassNotFoundException, URISyntaxException {

        String host = sock.getInetAddress().getHostAddress();
        int port = (Integer) inputStream.readObject();
        URI uri = new URI("hdfs://" + host + ":" + port);

        // Le noeud est-il connu ?
        if (this.nodes.stream().anyMatch(node -> node.equals(uri))) {
            // On envoie pong
            outputStream.writeObject(Action.PONG);
        } else {
            // On informe le noeud qu'il n'est pas initialisé
            System.out.println("Pong : Le ping provient d'un noeud inconnu...");
            outputStream.writeObject(Action.UNKNOWN_NODE);
        }

    }

    /**
     * Traite une requête de lecture.
     */
    private void handleRead(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws ClassNotFoundException, IOException {

        Metadata metadata = (Metadata) inputStream.readObject();
        if (!this.isFileComplete(metadata.getName())) {
            throw new RuntimeException("Fichier incomplet");
        }

        Map<Integer, List<URI>> file = this.files.get(metadata.getName());
        for (int fragment : file.keySet()) {
            URI node = file.get(fragment).get(0);
            Socket nodeSock = new Socket(node.getHost(), node.getPort());

            ObjectOutputStream nodeOutputStream = new ObjectOutputStream(nodeSock.getOutputStream());

            nodeOutputStream.writeObject(Action.READ);
            nodeOutputStream.writeObject(metadata.getName());
            nodeOutputStream.writeObject(fragment);

            ObjectInputStream nodeInputStream = new ObjectInputStream(nodeSock.getInputStream());

            while (true) {
                KV record = (KV) nodeInputStream.readObject();
                if (record == null) {
                    break;
                }
                outputStream.writeObject(record);
            }

            nodeOutputStream.writeObject(Action.PONG);

        }

        outputStream.writeObject(null);
        assert inputStream.readObject() == Action.PONG;

    }

    /**
     * Traite une requête d'écriture.
     */
    private void handleWrite(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws ClassNotFoundException, IOException {

        if (this.nodes.isEmpty()) {
            throw new RuntimeException("0 noeud");
        }

        Metadata metadata = (Metadata) inputStream.readObject();

        File tmp = File.createTempFile("hdfs-", ".tmp");
        tmp.deleteOnExit();

        Format writer = new KVFormatS(tmp.getAbsolutePath());
        writer.open(OpenMode.W);

        int lines = 0;

        while (true) {
            KV record = (KV) inputStream.readObject();
            if (record == null) {
                break;
            }
            writer.write(record);
            lines++;
        }
        writer.close();

        // On a reçu tout le fichier, on informe le client que c'est bon
        outputStream.writeObject(Action.PONG);

        // On le distribue aux noeuds
        int linesParFragment = (int) Math.ceil(lines / this.nodes.size());
        boolean lastPart = false;
        KV[] buffer = new KV[linesParFragment];

        Format reader = new KVFormatS(tmp.getAbsolutePath());
        reader.open(OpenMode.R);

        int fragment = 0;
        KV firstRecord = reader.read();

        if (firstRecord == null) {
            return;
        }

        // Tant qu'il reste des lignes
        while (!lastPart) {

            buffer[0] = firstRecord;

            for (int i = 1; i < linesParFragment; i++) {

                KV record = (KV) reader.read();
                buffer[i] = record;

                if (record == null) {
                    lastPart = true;
                    break;
                }

            }

            // On veut une gestion propre de la fin du fichier si elle est sur la fin d'un
            // fragment
            if (!lastPart) {
                firstRecord = (KV) reader.read();
                if (firstRecord == null) {
                    lastPart = true;
                }
            }

            this.sendFragment(metadata.getName(), fragment, lastPart, buffer);

            fragment++;
        }

        reader.close();

    }

    /**
     * Envoie un fragment au noeud
     *
     * @param fragment
     * @param lastPart
     * @param buffer
     */
    private void sendFragment(String fileName, int fragment, boolean lastPart, KV[] buffer) {

        // Nombre de noeuds sur lesquels chaque fragment est stocké
        // La fonctionnalité est implémentée, bien qu'inaccessible
        final int REP = 1;

        // Permutation
        URI[] nodes = Arrays.copyOf(this.nodes.toArray(), this.nodes.size(), URI[].class);
        int len = nodes.length;
        for (int i = 0; i < REP; i++) {
            URI tmp = nodes[i];
            nodes[i] = nodes[fragment % (len - i) + i];
            nodes[fragment % (len - i) + i] = tmp;
        }

        List<URI> nodes2 = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(nodes, 0, REP)));

        // Envoi
        for (URI node : nodes2) {

            try {
                Socket sock;
                sock = new Socket(node.getHost(), node.getPort());
                sock.setSoTimeout(1000);
                ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream());

                outputStream.writeObject(Action.WRITE);
                outputStream.writeObject(fileName);
                outputStream.writeObject(fragment);
                outputStream.writeObject(lastPart);
                for (KV record : buffer) {
                    if (record == null) {
                        break;
                    }
                    outputStream.writeObject(record);
                }
                outputStream.writeObject(null);

                if (inputStream.readObject() != Action.PONG) {
                    throw new SocketException("Noeud déconnecté.");
                }

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

        }

        // Enregistrement du noeud de chaque fragment
        if (!this.files.containsKey(fileName))
            this.files.put(fileName, new HashMap<>());

        this.files.get(fileName).put(fragment, nodes2);

    }

    /**
     * Traite une requête de suppression.
     */
    private void handleDelete(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws ClassNotFoundException, IOException {
        String filename = (String) inputStream.readObject();
        for (URI uri : new ArrayList<>(this.nodes)) {

            try {

                Socket nodeSock = new Socket(uri.getHost(), uri.getPort());
                ObjectOutputStream out = new ObjectOutputStream(nodeSock.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(nodeSock.getInputStream());

                // On envoie le nom du fichier à delete
                out.writeObject(Action.DELETE);
                out.writeObject(filename);

                if (in.readObject() != Action.PONG) {
                    throw new SocketException("Noeud déconnecté.");
                }

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        this.files.remove(filename);
        outputStream.writeObject(Action.PONG);
    }

    /**
     * Traite une requête de nouveau noeud, en récupérant son port et la liste de
     * ses fichiers
     */
    private void handleNewNode(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws IOException, ClassNotFoundException, URISyntaxException {

        String host = sock.getInetAddress().getHostAddress();
        int port = (int) inputStream.readObject();
        String root = (String) inputStream.readObject();

        System.out.println("Intialisation d'un nouveau noeud : " + host + ":" + port);

        URI uri = new URI("hdfs://" + host + ":" + port);
        this.removeNode(uri);
        this.nodes.add(uri);
        this.roots.put(uri, root);

        // On enregistre
        Object files = inputStream.readObject();
        this.registerFragments(uri, files);

        outputStream.writeObject(host);

        this.printFiles();

    }

    /**
     * Enregistre les fragments reçus
     *
     * @param uri   Noeud emetteur
     * @param files Objet reçu
     */
    private void registerFragments(URI uri, Object files) {
        for (Entry<?, ?> entry : ((Map<?, ?>) files).entrySet()) {
            String fileName = (String) entry.getKey();

            if (!this.files.containsKey(fileName)) {
                this.files.put((String) fileName, new HashMap<>());
            }
            Map<Integer, List<URI>> fragmentMap = this.files.get(fileName);

            for (Entry<?, ?> entry2 : ((Map<?, ?>) entry.getValue()).entrySet()) {
                int id = (int) entry2.getKey();
                if (!fragmentMap.containsKey(id)) {
                    fragmentMap.put(id, new ArrayList<>());
                }

                if (entry2.getValue() != null) {
                    List<URI> socketList = fragmentMap.get(id);
                    socketList.add(uri);
                }
            }
        }
    }

    /**
     * Affiche la liste des fichiers disponibles sur le réseau.
     */
    private void printFiles() {
        System.out.println("Fichiers :");
        boolean empty = true;
        for (String fileName : this.files.keySet()) {
            System.out.println(" * " + fileName + " : " + (this.isFileComplete(fileName) ? "complet" : "incomplet"));
            empty = false;
        }
        if (empty) {
            System.out.println(" (vide)");
        }
    }

    /**
     * Renvoie vrai si tous les fragments d'un fichier sont récupérables.
     *
     * @param fileName
     * @return
     */
    public boolean isFileComplete(String fileName) {
        if (!this.files.containsKey(fileName)) {
            return false;
        }
        Map<Integer, List<URI>> fragmentMap = this.files.get(fileName);
        int max = Collections.max(fragmentMap.keySet());
        for (int i = 0; i <= max; i++) {
            if (!fragmentMap.containsKey(i) || fragmentMap.get(i).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public static class FragmentInfo {
        public String filename;
        public int id;
        public boolean lastPart;
        public URI node;
        public String root;

        public FragmentInfo(String filename, int id, boolean lastPart, URI node, String root) {
            this.filename = filename;
            this.id = id;
            this.lastPart = lastPart;
            this.node = node;
            this.root = root;
        }

        public String getFragmentName() {
            return filename + "." + id + (lastPart ? ".final" : "") + ".part";
        }

        public String getAbsolutePath() {
            return new File(this.root, this.getFragmentName()).getAbsolutePath();
        }
    }

    private void handleListFragments(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws ClassNotFoundException, IOException {
        String filename = (String) inputStream.readObject();
        if (!this.isFileComplete(filename)) {
            outputStream.writeObject(null);
            return;
        }

        List<FragmentInfo> list = new ArrayList<>();
        Map<Integer, List<URI>> fragments = this.files.get(filename);
        int lastFragment = Collections.max(fragments.keySet());
        for (int id : fragments.keySet()) {
            URI node = fragments.get(id).get(0);
            list.add(new FragmentInfo(filename, id, id == lastFragment, node, this.roots.get(node)));
        }

        outputStream.writeObject(list);
        assert inputStream.readObject() == Action.PONG;

    }

    /**
     * Traite une demande de mise à jour de la liste des fichiers.
     */
    private void handleForceRescan(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        this.files = new HashMap<>();

        for (URI node : this.nodes) {
            try {
                Socket nodeSock = new Socket(node.getHost(), node.getPort());
                ObjectOutputStream out = new ObjectOutputStream(nodeSock.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(nodeSock.getInputStream());

                out.writeObject(Action.FORCE_RESCAN);
                this.registerFragments(node, in.readObject());

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        try {
            outputStream.writeObject(Action.PONG);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.printFiles();
    }

    /**
     * Interface CLI pour lancer un serveur.
     */
    public static void main(String[] args) throws IOException {
        if (args.length > 0 && (args[0].equalsIgnoreCase("--help") || args[0].equalsIgnoreCase("-h")
                || args[0].equals("-?") || args[0].equals("/?"))) {
            System.out.println("Usage: HdfsNameServer <optional port>");
            return;
        }

        int port = DEFAULT_PORT;
        if (args.length == 1) {
            port = Integer.parseInt(args[0]);
        }
        new HdfsNameServer(port);
    }

}
