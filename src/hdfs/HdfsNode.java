package hdfs;

import formats.Format;
import formats.KV;
import formats.LineFormat;
import hdfs.HdfsNameServer.Action;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class HdfsNode {

    /**
     * Racine des fichiers du noeud.
     */
    private String nodeRoot;

    /**
     * Serveur du noeud.
     */
    private ServerSocket server;

    /**
     * Hôte du NameServer.
     */
    private String nameServerHost;

    /**
     * Port du NameServer.
     */
    private int nameServerPort;

    /**
     * Liste des fichiers.
     */
    private Map<String, Map<Integer, File>> files;

    /**
     * Adresse du noeud depuis le NameServer.
     */
    private String externalHostname;

    /**
     * Initialise un noeud connecté au NameServer host:port
     */
    public HdfsNode(String host, int port, String nodeRoot) {
        this.nameServerHost = host;
        this.nameServerPort = port;
        this.nodeRoot = new File(nodeRoot).getAbsolutePath();

        // On crée un serveur
        try {
            this.openServer();
        } catch (IOException e) {
            // TODO Meilleur intégration avec le BiNode
            System.out.println("Impossible d'obtenir un port libre.");
            return;
        }

        // On scanne le dossier courant
        this.scanDir();

        // On contacte le NameServer
        try {
            this.initNode();
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
            return;
        }

        System.out.println();
        System.out.println("Initialisation :");
        System.out.println("* Serveur lancé sur le port " + this.server.getLocalPort());
        System.out.println("* Ctrl+C pour arrêter le serveur");
        System.out.println("* Dossier courant : " + this.nodeRoot);
        System.out.println();

    }

    /**
     * Lance les 2 threads du serveur.
     */
    public void run() {
        this.runPinger();

        this.runListener();
    }

    private void initNode() {

        try {
            Socket sock = this.newNameServerSocket();

            ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());

            outputStream.writeObject(Action.NEW_NODE);
            outputStream.writeObject(this.server.getLocalPort());
            outputStream.writeObject(this.nodeRoot);
            outputStream.writeObject(this.files);

            this.externalHostname = (String) new ObjectInputStream(sock.getInputStream()).readObject();

        } catch (IOException | AssertionError | ClassNotFoundException e) {
            // TODO check ça
            throw new RuntimeException("Le NameServer n'est pas joignable.");
        }

    }

    /**
     * Crée une socket vers le NameServer.
     */
    private Socket newNameServerSocket() throws UnknownHostException, IOException {
        return new Socket(this.nameServerHost, this.nameServerPort);
    }

    /**
     * Crée le serveur sur un port aléatoire.
     *
     * @throws IOException
     */
    private void openServer() throws IOException {
        this.server = new ServerSocket(0);
    }

    /**
     * Scanne le dossier du noeud pour découvrir les fichiers stockés.
     */
    private void scanDir() {
        this.files = new HashMap<>();

        for (File f : new File(this.nodeRoot).listFiles()) {
            // On parse le nom du fichier
            String name = f.getName();
            if (!f.isFile() || !name.endsWith(".part")) {
                continue;
            }
            name = name.substring(0, name.length() - 5);
            boolean lastPart = name.endsWith(".final");
            if (lastPart) {
                name = name.substring(0, name.length() - 6);
            }
            int id;
            String originalName;
            try {
                int pos = name.lastIndexOf(".");
                id = Integer.parseInt(name.substring(pos + 1));
                originalName = name.substring(0, pos);
            } catch (NumberFormatException e) {
                continue;
            }

            // On sauvegarde le fragment
            if (!files.containsKey(originalName)) {
                files.put(originalName, new HashMap<>());
            }

            Map<Integer, File> fragmentMap = files.get(originalName);

            fragmentMap.put(id, f.getAbsoluteFile());

            if (!lastPart && !fragmentMap.containsKey(id + 1)) {
                fragmentMap.put(id + 1, null);
            }
        }

    }

    /**
     * Lance l'écoute sur le serveur.
     */
    private void runListener() {

        while (true) {
            try {
                Socket sock = this.server.accept();

                ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream());
                ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());

                HdfsNode self = this;
                // TODO C'est propre ça ?
                new Thread(new Runnable() {
                    public void run() {
                        self.handleRequest(sock, inputStream, outputStream);
                    }
                }).start();

            } catch (IOException e) {
                // TODO Abandonner la requête proprement
                e.printStackTrace();
            }
        }

    }

    private void handleRequest(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream) {

        try {

            Action action = (Action) inputStream.readObject();

            if (action == Action.PING) {
                outputStream.writeObject(Action.PONG);
            } else if (action == Action.WRITE) {
                this.handleWrite(sock, inputStream, outputStream);
            } else if (action == Action.READ) {
                this.handleRead(sock, inputStream, outputStream);
            } else if (action == Action.DELETE) {
                this.handleDelete(sock, inputStream, outputStream);
            } else if (action == Action.FORCE_RESCAN) {
                this.handleForceRescan(sock, inputStream, outputStream);
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            // TODO Abandonner la requête proprement
        }
    }

    private void handleRead(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws ClassNotFoundException, IOException {

        String fileName = (String) inputStream.readObject();
        int fragment = (int) inputStream.readObject();
        File file = this.files.get(fileName).get(fragment);

        OutputStream os = sock.getOutputStream();
        os.write(Files.readAllBytes(Path.of(file.getAbsolutePath())));
        os.close();

        // TODO Gestion du pong
        assert inputStream.readObject() == Action.PONG;
        sock.close();
    }

    private void handleWrite(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws ClassNotFoundException, IOException {

        String fileName = (String) inputStream.readObject();
        int fragment = (int) inputStream.readObject();
        boolean lastPart = (boolean) inputStream.readObject();

        // TODO Extraire la génération des noms
        Format writer = new LineFormat(new File(this.nodeRoot, fileName).getAbsolutePath() + "." + fragment
                + (lastPart ? ".final" : "") + ".part");
        writer.open(Format.OpenMode.W);

        // TODO Changer le mode d'envoi des données
        while (true) {
            KV record = (KV) inputStream.readObject();
            if (record == null) {
                break;
            }
            writer.write(record);
        }

        writer.close();

        outputStream.writeObject(Action.PONG);
        this.scanDir();

    }

    private void handleDelete(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        try {
            String filename = (String) inputStream.readObject();
            if (this.files.containsKey(filename)) {
                for (File fragment : this.files.get(filename).values()) {
                    if (fragment != null) {
                        fragment.delete();
                    }
                }
            }
            this.files.remove(filename);
            outputStream.writeObject(Action.PONG);
        } catch (ClassNotFoundException | IOException e) {
            // TODO Gérer proprement
            e.printStackTrace();
        }
    }

    /**
     * On envoie la liste des fichiers.
     *
     * @throws IOException
     */
    private void handleForceRescan(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws IOException {
        this.scanDir();
        outputStream.writeObject(this.files);
    }

    /**
     * Lance le service de vérification de l'activité du NameServer.
     */
    private void runPinger() {
        // TODO Oskour
        HdfsNode self = this;
        class Pinger implements Runnable {
            @Override
            public void run() {
                while (true) {
                    self.sendPing();
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        }
        new Thread(new Pinger()).start();
    }

    /**
     * Vérifie que le NameServer est en ligne.
     */
    public void sendPing() {
        try {
            Socket sock = this.newNameServerSocket();

            ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream());

            outputStream.writeObject(Action.PING);
            outputStream.writeObject(this.server.getLocalPort());

            Object answer = inputStream.readObject();

            if (answer != Action.PONG) {
                System.out.println("Ping : Le NameServer ne reconnaît pas le noeud, initialisation...");
                this.initNode();
            }

        } catch (AssertionError e) {
        } catch (IOException | ClassNotFoundException e) {
            // TODO augmenter l'attente ?
            System.out.println("Ping : Le NameServer n'est pas joignable.");
        }
    }

    /**
     * Getter du serveur d'écoute.
     */
    public ServerSocket getServer() {
        return this.server;
    }

    /**
     * Getter du nom d'hôte du noeud vu depuis le NameServer.
     */
    public String getExternalHostname() {
        return this.externalHostname;
    }

    /**
     * Permet l'instanciation d'un noeud en CLI.
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length > 0 && (args[0].equalsIgnoreCase("--help") || args[0].equalsIgnoreCase("-h")
                || args[0].equals("-?") || args[0].equals("/?")) || args.length == 0) {
            printUsage();
            return;
        }

        URI uri;
        try {
            uri = new URI("hdfs://" + args[0]);
            if (uri.getHost() == null || uri.getPort() == -1) {
                throw new URISyntaxException(uri.toString(), "URI must have host and port parts");
            }
        } catch (URISyntaxException e) {
            System.out.println("Argument incorrect.");
            printUsage();
            return;
        }

        String nodeRoot = ".";

        if (args.length >= 2) {
            nodeRoot = args[1];
        }

        new HdfsNode(uri.getHost(), uri.getPort(), nodeRoot).run();
    }

    /**
     * Affiche les paramètres CLI.
     */
    private static void printUsage() {
        System.out.println("Usage: HdfsNode <master_host:master_port> <root>");
    }

}
