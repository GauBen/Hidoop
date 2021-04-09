/**
 * HDFS - Hidoop Distributed File System.
 *
 * Serveur développé par Théo Petit et Gautier Ben Aïm.
 */

package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Serveur HDFS, capable d'initier des opérations distribuées de lecture,
 * d'écriture et de suppression sur les noeuds.
 */
public class HdfsNameServer {

    /**
     * Nom d'hôte par défaut du serveur.
     */
    final public static String DEFAULT_HOST = "127.0.0.1";

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
    final public static int BUFFER_SIZE = 4194304;

    /**
     * Serveur qui traite les requêtes HDFS.
     */
    private ServerSocket server;

    /**
     * Ensemble des noeuds.
     */
    private volatile Set<HdfsNodeInfo> nodes = new HashSet<>();

    /**
     * Liste des fichiers.
     */
    private volatile Map<String, Map<Integer, Set<HdfsNodeInfo>>> files = new HashMap<>();

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
        System.out.println();
        try {
            this.server = new ServerSocket(port);
            System.out.println("Initialisation :");
            System.out.println("* Serveur principal lancé sur le port " + this.server.getLocalPort());
            this.runPinger();
            System.out.println("* Service de ping démarré");
            System.out.println("[Ctrl+C pour arrêter le serveur]");
            System.out.println();
            this.runListener();
        } catch (IOException e) {
            System.err
                    .println("Impossible de lancer le serveur sur le port " + port + ", le port est peut-être occupé.");
            throw new HdfsRuntimeException(e);
        }
    }

    /**
     * Crée un thread qui vérifie que les noeuds sont actifs.
     */
    private void runPinger() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    HdfsNameServer.this.sendPings();
                    try {
                        Thread.sleep(PING_INTERVAL);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }).start();
    }

    /**
     * Envoie un ping à tous les noeuds.
     */
    public void sendPings() {

        // Nombre de noeuds supprimés
        int removed = 0;

        for (HdfsNodeInfo node : new HashSet<>(this.nodes)) {

            try (Socket sock = new Socket(node.getHost(), node.getPort())) {

                sock.setSoTimeout(1000);

                // On envoie ping et on attend pong
                ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());
                outputStream.writeObject(HdfsAction.PING);

                ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream());
                expectPong(inputStream);

            } catch (IOException | ClassNotFoundException e) {

                System.err.println("Ping : noeud " + node + " déconnecté...");

                this.removeNode(node);
                removed++;

            }

        }

        // S'il y a plus d'un noeud supprimé on affiche la liste des fichiers
        if (removed > 0) {
            this.printFiles();
        }

    }

    /**
     * Attend un pong ou lance une exception.
     *
     * @param inputStream
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws SocketException
     */
    private void expectPong(ObjectInputStream inputStream) throws IOException, ClassNotFoundException, SocketException {
        if (inputStream.readObject() != HdfsAction.PONG) {
            throw new SocketException();
        }
    }

    /**
     * Supprime la référence à un noeud dans la liste des noeuds et des fichiers.
     *
     * @param node Une adresse de la forme hdfs://adresse:port
     */
    private void removeNode(HdfsNodeInfo node) {

        Set<HdfsNodeInfo> toRemove = this.nodes.stream().filter(n -> n.matches(node)).collect(Collectors.toSet());
        this.nodes.removeAll(toRemove);

        for (Map<Integer, Set<HdfsNodeInfo>> map : this.files.values()) {
            for (Set<HdfsNodeInfo> set : map.values()) {
                set.removeAll(toRemove);
            }
        }

    }

    /**
     * Lance l'attente des requêtes entrantes.
     */
    private void runListener() {
        while (true) {
            try (Socket sock = this.server.accept()) {
                // On traite la requête entrante
                this.handleRequest(sock);
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Une connexion a échoué.");
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
    private void handleRequest(Socket sock) {
        try (ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream())) {
            HdfsAction action = (HdfsAction) inputStream.readObject();

            // On filtre l'action demandée
            if (action == HdfsAction.PING) {
                this.handlePing(sock, inputStream);
            } else if (action == HdfsAction.READ) {
                this.handleRead(sock, inputStream);
            } else if (action == HdfsAction.WRITE) {
                this.handleWrite(sock, inputStream);
            } else if (action == HdfsAction.DELETE) {
                this.handleDelete(sock, inputStream);
            } else if (action == HdfsAction.NEW_NODE) {
                this.handleNewNode(sock, inputStream);
            } else if (action == HdfsAction.LIST_FRAGMENTS) {
                this.handleListFragments(sock, inputStream);
            } else if (action == HdfsAction.FORCE_RESCAN) {
                this.handleForceRescan(sock, inputStream);
            } else {
                throw new IllegalArgumentException("Action invalide.");
            }

        } catch (ClassNotFoundException | IOException e) {
            // TODO check ça
            e.printStackTrace();
            System.err.println("Données invalides, connexion annulée.");
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            System.err.println("Action reçue invalide.");
        }
    }

    /**
     * Réceptionne un ping. Si le noeud est connu, rien ne change, sinon on demande
     * au noeud de s'initialiser.
     */
    private void handlePing(Socket sock, ObjectInputStream inputStream) {

        try (ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream())) {

            String host = sock.getInetAddress().getHostAddress();
            int port = (Integer) inputStream.readObject();

            // Le noeud est-il connu ?
            if (this.nodes.stream().anyMatch(node -> node.matches(host, port))) {
                // On envoie pong
                outputStream.writeObject(HdfsAction.PONG);
            } else {
                // On informe le noeud qu'il n'est pas initialisé
                System.err.println("Ping reçu d'un noeud inconnu.");
                outputStream.writeObject(HdfsAction.UNKNOWN_NODE);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
        }

    }

    /**
     * Traite une requête de lecture.
     */
    private void handleRead(Socket sock, ObjectInputStream inputStream) {

        try (BufferedOutputStream bos = new BufferedOutputStream(sock.getOutputStream())) {

            String name = (String) inputStream.readObject();
            if (!this.isFileComplete(name)) {
                // TODO transmettre l'erreur
                throw new RuntimeException("Fichier incomplet");
            }

            Map<Integer, Set<HdfsNodeInfo>> file = this.files.get(name);
            for (int fragment : file.keySet()) {
                HdfsNodeInfo node = file.get(fragment).iterator().next();

                try (Socket nodeSock = new Socket(node.getHost(), node.getPort())) {

                    ObjectOutputStream nodeOutputStream = new ObjectOutputStream(nodeSock.getOutputStream());

                    nodeOutputStream.writeObject(HdfsAction.READ);
                    nodeOutputStream.writeObject(name);
                    nodeOutputStream.writeObject(fragment);

                    BufferedInputStream rawInput = new BufferedInputStream(nodeSock.getInputStream());
                    rawInput.transferTo(bos);
                    bos.flush();

                    nodeOutputStream.writeObject(HdfsAction.PONG);
                } catch (UnknownHostException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }

            sock.shutdownOutput();
            expectPong(inputStream);

        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
        }
    }

    /**
     * Traite une requête d'écriture.
     */
    private void handleWrite(Socket sock, ObjectInputStream inputStream) {

        if (this.nodes.isEmpty()) {
            // TODO propagation de l'exception
            throw new RuntimeException("0 noeud");
        }

        try (ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());
                BufferedInputStream rawInput = new BufferedInputStream(sock.getInputStream())) {

            String name = (String) inputStream.readObject();
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            int fragment = 0;
            int input;

            while (rawInput.available() > 0) {
                buffer.reset();
                buffer.write(rawInput.readNBytes(BUFFER_SIZE));
                while ((input = rawInput.read()) != -1) {
                    buffer.write(input);
                    if ((char) input == '\n') {
                        break;
                    }
                }

                sendFragment(name, fragment, rawInput.available() == 0, buffer);

                fragment++;
            }

            outputStream.writeObject(HdfsAction.PONG);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Envoie un fragment au noeud
     *
     * @param fragment
     * @param lastPart
     * @param buffer
     */
    private void sendFragment(String fileName, int fragment, boolean lastPart, ByteArrayOutputStream bytes) {

        // Nombre de noeuds sur lesquels chaque fragment est stocké
        // La fonctionnalité est implémentée, bien qu'inaccessible
        final int REP = 2;

        // Permutation
        List<HdfsNodeInfo> permutation = new ArrayList<>(nodes);
        permutation.addAll(nodes);
        int startIndex = fragment % nodes.size();
        List<HdfsNodeInfo> pickedNodes = permutation.subList(startIndex, startIndex + REP);

        Set<HdfsNodeInfo> successfulNodes = new HashSet<>(pickedNodes);

        // Envoi
        for (HdfsNodeInfo node : pickedNodes) {

            try (Socket sock = new Socket(node.getHost(), node.getPort());
                    BufferedOutputStream rawOutputStream = new BufferedOutputStream(sock.getOutputStream());
                    ObjectOutputStream outputStream = new ObjectOutputStream(rawOutputStream)) {

                sock.setSoTimeout(1000);

                outputStream.writeObject(HdfsAction.WRITE);
                outputStream.writeObject(fileName);
                outputStream.writeObject(fragment);
                outputStream.writeObject(lastPart);
                bytes.writeTo(rawOutputStream);

                rawOutputStream.flush();
                sock.shutdownOutput();

                expectPong(new ObjectInputStream(sock.getInputStream()));

            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Erreur de connexion avec le noeud " + node);
                successfulNodes.remove(node);
                removeNode(node);
            }

        }

        // Enregistrement du noeud de chaque fragment
        if (!this.files.containsKey(fileName))
            this.files.put(fileName, new HashMap<>());

        this.files.get(fileName).put(fragment, successfulNodes);

        if (successfulNodes.size() < REP) {
            // TODO
        }

    }

    /**
     * Traite une requête de suppression.
     */
    private void handleDelete(Socket sock, ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
        String filename = (String) inputStream.readObject();
        for (HdfsNodeInfo uri : new HashSet<>(this.nodes)) {

            try {

                Socket nodeSock = new Socket(uri.getHost(), uri.getPort());
                ObjectOutputStream out = new ObjectOutputStream(nodeSock.getOutputStream());

                // On envoie le nom du fichier à delete
                out.writeObject(HdfsAction.DELETE);
                out.writeObject(filename);

                expectPong(new ObjectInputStream(nodeSock.getInputStream()));
            } catch (IOException | ClassNotFoundException e) {
                // TODO Déconnecter les noeuds proprement
                e.printStackTrace();
            }
        }
        this.files.remove(filename);

        ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());
        outputStream.writeObject(HdfsAction.PONG);
    }

    /**
     * Traite une requête de nouveau noeud, en récupérant son port et la liste de
     * ses fichiers
     */
    private void handleNewNode(Socket sock, ObjectInputStream inputStream) throws IOException, ClassNotFoundException {

        ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());

        String host = sock.getInetAddress().getHostAddress();
        int port = (int) inputStream.readObject();
        String root = (String) inputStream.readObject();

        System.out.println("Intialisation d'un nouveau noeud : " + host + ":" + port);

        HdfsNodeInfo node = new HdfsNodeInfo(host, port, root);
        this.removeNode(node);
        this.nodes.add(node);

        // On enregistre
        Object files = inputStream.readObject();
        this.registerFragments(node, files);

        outputStream.writeObject(host);

        this.printFiles();

    }

    /**
     * Enregistre les fragments reçus
     *
     * @param node  Noeud emetteur
     * @param files Objet reçu
     */
    private void registerFragments(HdfsNodeInfo node, Object files) {
        for (Entry<?, ?> entry : ((Map<?, ?>) files).entrySet()) {
            String fileName = (String) entry.getKey();

            if (!this.files.containsKey(fileName)) {
                this.files.put((String) fileName, new HashMap<>());
            }
            Map<Integer, Set<HdfsNodeInfo>> fragmentMap = this.files.get(fileName);

            for (Entry<?, ?> entry2 : ((Map<?, ?>) entry.getValue()).entrySet()) {
                int id = (int) entry2.getKey();
                if (!fragmentMap.containsKey(id)) {
                    fragmentMap.put(id, new HashSet<>());
                }

                if (entry2.getValue() != null) {
                    Set<HdfsNodeInfo> socketList = fragmentMap.get(id);
                    socketList.add(node);
                }
            }
        }
    }

    /**
     * Affiche la liste des fichiers disponibles sur le réseau.
     */
    private void printFiles() {
        // TODO Meilleur affichage
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
        Map<Integer, Set<HdfsNodeInfo>> fragmentMap = this.files.get(fileName);
        int max = Collections.max(fragmentMap.keySet());
        for (int i = 0; i <= max; i++) {
            if (!fragmentMap.containsKey(i) || fragmentMap.get(i).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public static class FragmentInfo implements Serializable {
        // TODO extraire cette classe
        private static final long serialVersionUID = -1636990109710437159L;
        public String filename;
        public int id;
        public boolean lastPart;
        public HdfsNodeInfo node;
        public String root;

        public FragmentInfo(String filename, int id, boolean lastPart, HdfsNodeInfo node, String root) {
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

    private void handleListFragments(Socket sock, ObjectInputStream inputStream)
            throws ClassNotFoundException, IOException {

        ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());

        String filename = (String) inputStream.readObject();
        if (!this.isFileComplete(filename)) {
            outputStream.writeObject(null);
            return;
        }

        List<List<FragmentInfo>> list = new ArrayList<>();
        Map<Integer, Set<HdfsNodeInfo>> fragments = this.files.get(filename);
        int lastFragment = Collections.max(fragments.keySet());
        for (int id : fragments.keySet()) {
            Set<HdfsNodeInfo> node = fragments.get(id);
            list.add(node.stream().map(uri -> new FragmentInfo(filename, id, id == lastFragment, uri, uri.getRoot()))
                    .collect(Collectors.toList()));
        }

        outputStream.writeObject(list);
        expectPong(inputStream);

    }

    /**
     * Traite une demande de mise à jour de la liste des fichiers.
     *
     * @throws IOException
     */
    private void handleForceRescan(Socket sock, ObjectInputStream inputStream) throws IOException {
        this.files = new HashMap<>();

        for (HdfsNodeInfo node : this.nodes) {
            try (Socket nodeSock = new Socket(node.getHost(), node.getPort());
                    ObjectOutputStream out = new ObjectOutputStream(nodeSock.getOutputStream());) {

                out.writeObject(HdfsAction.FORCE_RESCAN);

                ObjectInputStream in = new ObjectInputStream(nodeSock.getInputStream());
                this.registerFragments(node, in.readObject());

            } catch (IOException | ClassNotFoundException e) {
                // TODO Déconnecter les noeuds proprement
                e.printStackTrace();
            }
        }

        ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());
        outputStream.writeObject(HdfsAction.PONG);

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
