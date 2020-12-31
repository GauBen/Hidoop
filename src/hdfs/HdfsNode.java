package hdfs;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import formats.Format;
import formats.KV;
import formats.LineFormat;
import formats.LineFormatS;
import hdfs.HdfsNameServer.Action;

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
     * Initialise un noeud connecté au NameServer host:port
     */
    HdfsNode(String host, int port, String nodeRoot) {
        this.nameServerHost = host;
        this.nameServerPort = port;
        this.nodeRoot = new File(nodeRoot).getAbsolutePath();

        // On crée un serveur
        try {
            this.openServer();
        } catch (IOException e) {
            System.out.println("Impossible d'obtenir un port libre.");
            return;
        }

        // On scanne le dossier courant
        this.scanDir();

        // On contacte le NameServer
        try {
            System.out.println("Contact...");
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

        this.runPinger();

        this.runListener();
    }

    private void initNode() {

        try {
            Socket sock = this.newNameServerSocket();

            ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());

            outputStream.writeObject(Action.NEW_NODE);
            outputStream.writeObject(this.server.getLocalPort());
            outputStream.writeObject(this.files);

        } catch (IOException | AssertionError e) {
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

                this.handleRequest(sock, inputStream, outputStream);

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

    }

    private void handleRequest(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws IOException, ClassNotFoundException {

        Action action = (Action) inputStream.readObject();

        if (action == Action.PING) {
            outputStream.writeObject(Action.PONG);
        } else if (action == Action.WRITE) {
            this.handleWrite(sock, inputStream, outputStream);
        } else if (action == Action.READ) {
            this.handleRead(sock, inputStream, outputStream);
        }
    }

    private void handleRead(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws ClassNotFoundException, IOException {

        String fileName = (String) inputStream.readObject();
        int fragment = (int) inputStream.readObject();
        File file = this.files.get(fileName).get(fragment);

        Format reader = new LineFormatS(file.getAbsolutePath());
        reader.open(Format.OpenMode.R);
        while (true) {
            KV record = (KV) reader.read();
            outputStream.writeObject(record);
            if (record == null) {
                break;
            }
        }
        reader.close();

        assert inputStream.readObject() == Action.PONG;
    }

    private void handleWrite(Socket sock, ObjectInputStream inputStream, ObjectOutputStream outputStream)
            throws ClassNotFoundException, IOException {

        String fileName = (String) inputStream.readObject();
        int fragment = (int) inputStream.readObject();
        boolean lastPart = (boolean) inputStream.readObject();

        Format writer = new LineFormat(new File(this.nodeRoot, fileName).getAbsolutePath() + "." + fragment
                + (lastPart ? ".final" : "") + ".part");
        writer.open(Format.OpenMode.W);
        // String fileName = "./node-1/" + metadata.getFragmentName();
        // Format writer = metadata.getFormat() == Type.KV ? new KVFormat(fileName) :
        // new LineFormat(fileName);

        while (true) {
            KV record = (KV) inputStream.readObject();
            if (record == null) {
                break;
            }
            writer.write(record);
        }

        writer.close();

        outputStream.writeObject(Action.PONG);

    }

    /**
     * Lance le service de vérification de l'activité du NameServer.
     */
    private void runPinger() {
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
            System.out.println("Ping : Le NameServer n'est pas joignable.");
        }
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

        new HdfsNode(uri.getHost(), uri.getPort(), nodeRoot);
    }

    /**
     * Affiche les paramètres CLI.
     */
    private static void printUsage() {
        System.out.println("Usage: HdfsNode <master_host:master_port> <root>");
    }

}
