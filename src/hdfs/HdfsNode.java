package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
            System.out.println("Impossible d'obtenir un port libre.");
            throw new HdfsRuntimeException(e);
        }

        // On scanne le dossier courant
        this.scanDir();

        // On contacte le NameServer
        this.initNode();

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

            System.out.println(
                    "Initialisation du serveur: port=" + this.server.getLocalPort() + "; root=" + this.nodeRoot);

            outputStream.writeObject(HdfsAction.NEW_NODE);
            outputStream.writeObject(this.server.getLocalPort());
            outputStream.writeObject(this.nodeRoot);
            outputStream.writeObject(this.files);

            this.externalHostname = (String) new ObjectInputStream(sock.getInputStream()).readObject();

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Le NameServer n'est pas joignable.");
            throw new HdfsRuntimeException(e);
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
            synchronized (this) {
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

                try {
                    if (!lastPart && Files.size(f.toPath()) < HdfsNameServer.BUFFER_SIZE) {
                        Files.delete(f.toPath());
                        continue;
                    }
                } catch (IOException e) {
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

    }

    /**
     * Lance l'écoute sur le serveur.
     */
    private void runListener() {

        ExecutorService executor = (ExecutorService) Executors.newCachedThreadPool();

        while (true) {
            try {

                Socket sock = this.server.accept();
                executor.submit(new Runnable() {
                    public void run() {
                        HdfsNode.this.handleRequest(sock);
                    }
                });

            } catch (IOException e) {
                System.err.println("Une connexion en erreur a été ignorée.");
            }
        }

    }

    private void handleRequest(Socket sock) {

        try (ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream())) {

            HdfsAction action = (HdfsAction) inputStream.readObject();

            if (action == HdfsAction.PING) {
                ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());
                outputStream.writeObject(HdfsAction.PONG);
            } else if (action == HdfsAction.WRITE) {
                this.handleWrite(sock, inputStream);
            } else if (action == HdfsAction.READ) {
                this.handleRead(sock, inputStream);
            } else if (action == HdfsAction.DELETE) {
                this.handleDelete(sock, inputStream);
            } else if (action == HdfsAction.FORCE_RESCAN) {
                this.handleForceRescan(sock, inputStream);
            } else {
                System.err.println("Action reçue invalide, connexion annulée.");
            }

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Données invalides, connexion annulée.");
        }
    }

    private void handleRead(Socket sock, ObjectInputStream inputStream) throws ClassNotFoundException, IOException {

        String fileName = (String) inputStream.readObject();
        int fragment = (int) inputStream.readObject();
        File file = this.files.get(fileName).get(fragment);

        ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream(sock.getOutputStream()));
        os.writeLong(Files.size(file.toPath()));
        Files.copy(file.toPath(), os);
        os.flush();
        sock.shutdownOutput();

        if (inputStream.readObject() != HdfsAction.PONG) {
            System.err.println("Pong non reçu, le serveur de nom a peut-être rencontré une erreur");
        }
        sock.close();
    }

    private void handleWrite(Socket sock, ObjectInputStream inputStream) throws ClassNotFoundException, IOException {

        ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());
        BufferedInputStream rawInput = new BufferedInputStream(sock.getInputStream());

        String fileName = (String) inputStream.readObject();
        int fragment = (int) inputStream.readObject();
        boolean lastPart = (boolean) inputStream.readObject();

        File f = new File(this.nodeRoot, FragmentInfo.makeFragmentName(fileName, fragment, lastPart));
        Files.copy(rawInput, f.toPath(), StandardCopyOption.REPLACE_EXISTING);

        outputStream.writeObject(HdfsAction.PONG);
        this.scanDir();

    }

    private void handleDelete(Socket sock, ObjectInputStream inputStream) {
        boolean done = false;
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
            done = true;
            ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());
            outputStream.writeObject(HdfsAction.PONG);
        } catch (ClassNotFoundException | IOException e) {
            if (done) {
                System.err.println(
                        "Erreur de connexion lors de la suppression, la suppression a quand même été effectuée.");
            } else {
                System.err.println("Erreur de connexion lors de la suppression, opération ignorée.");
            }
        }
    }

    /**
     * On envoie la liste des fichiers.
     *
     * @throws IOException
     */
    private void handleForceRescan(Socket sock, ObjectInputStream inputStream) throws IOException {
        this.scanDir();
        ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());
        outputStream.writeObject(this.files);
    }

    /**
     * Lance le service de vérification de l'activité du NameServer.
     */
    private void runPinger() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    HdfsNode.this.sendPing();
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        }).start();
    }

    /**
     * Vérifie que le NameServer est en ligne.
     */
    public void sendPing() {
        try {
            Socket sock = this.newNameServerSocket();

            ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());

            outputStream.writeObject(HdfsAction.PING);
            outputStream.writeObject(this.server.getLocalPort());

            ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream());
            Object answer = inputStream.readObject();

            if (answer != HdfsAction.PONG) {
                System.err.println("Ping : Le NameServer ne reconnaît pas le noeud, initialisation...");
                this.initNode();
            }

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Ping : Le NameServer n'est pas joignable.");
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
