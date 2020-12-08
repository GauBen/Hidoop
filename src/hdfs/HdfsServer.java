package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.Type;
import hdfs.HdfsClient.Action;

/**
 * Serveur HDFS, capable d'effectuer des opérations distribuées de lecture,
 * d'écriture et de suppression.
 */
public class HdfsServer {

    /**
     * Serveur qui traite les requêtes HDFS.
     */
    private ServerSocket server;

    /**
     * Initialise un noeud HDFS sur le port 8123.
     */
    public HdfsServer() {
        int port = 8123;
        try {
            this.server = new ServerSocket(port);
            System.out.println("Serveur lancé sur le port " + this.server.getLocalPort() + ".");
            System.out.println("Ctrl+C pour arrêter le serveur.");
            this.run();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Impossible de lancer le serveur, le port est peut-être occupé.");
        }
    }

    /**
     * Lance l'attente des chaussettes.
     */
    private void run() {
        while (true) {
            Socket sock = null;
            try {

                // On attend une connexion au serveur HDFS
                sock = this.server.accept();
                ObjectInputStream stream = new ObjectInputStream(sock.getInputStream());
                this.handleRequest(stream);
                sock.close();

            } catch (IOException e) {
                if (sock != null) {
                    System.out.println("Socket " + sock.getInetAddress() + " interrompue.");
                } else {
                    System.out.println("Une connexion a échoué.");
                }
                e.printStackTrace();
            }
        }
    }

    /**
     * On traite les chaussettes ouvertes.
     *
     * @param stream Flux d'objets entrants
     */
    private void handleRequest(ObjectInputStream stream) {
        try {
            Action action = (Action) stream.readObject();
            Metadata metadata = (Metadata) stream.readObject();

            System.out.println(metadata);

            if (action == Action.READ) {
                this.handleRead(metadata, stream);
            } else if (action == Action.WRITE) {
                this.handleWrite(metadata, stream);
            } else if (action == Action.DELETE) {
                this.handleDelete(metadata);
            } else {
                throw new IllegalArgumentException("Action invalide.");
            }

        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
    }

    private void handleRead(Metadata metadata, ObjectInputStream stream) throws ClassNotFoundException, IOException {
    }

    private void handleWrite(Metadata metadata, ObjectInputStream stream) throws ClassNotFoundException, IOException {
        Type type = (Type) stream.readObject();
        String fileName = "./node-1/" + metadata.getFragmentName();
        Format writer = type == Type.KV ? new KVFormat(fileName) : new LineFormat(fileName);
        writer.open(Format.OpenMode.W);
        while (true) {
            KV record = (KV) stream.readObject();
            if (record == null) {
                break;
            }
            writer.write(record);
        }
        writer.close();
    }

    private void handleDelete(Metadata metadata) throws ClassNotFoundException, IOException {
    }

    public static void main(String[] args) throws IOException {
        new HdfsServer();
    }

}
