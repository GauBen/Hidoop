/**
 * HDFS - Hidoop Distributed File System
 *
 * Serveur développé par Théo Petit et Gautier Ben Aïm
 */

package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.KVFormatS;
import formats.LineFormat;
import formats.LineFormatS;
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
                ObjectInputStream inputStream = new ObjectInputStream(sock.getInputStream());
                ObjectOutputStream outputStream = new ObjectOutputStream(sock.getOutputStream());

                // On traite la requête entrante
                this.handleRequest(inputStream, outputStream);

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
     * @param inputStream  Flux d'objets entrants
     * @param outputStream Flux d'objets sortants
     */
    private void handleRequest(ObjectInputStream inputStream, ObjectOutputStream outputStream) {
        try {
            Action action = (Action) inputStream.readObject();
            Metadata metadata = (Metadata) inputStream.readObject();

            System.out.println(metadata);

            if (action == Action.READ) {
                this.handleRead(metadata, outputStream);
            } else if (action == Action.WRITE) {
                this.handleWrite(metadata, inputStream);
            } else if (action == Action.DELETE) {
                this.handleDelete(metadata);
            } else {
                throw new IllegalArgumentException("Action invalide.");
            }

        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Traite une requête de lecture.
     *
     * @param metadata Informations sur le fichier à lire
     * @param stream   Flux inscriptible pour envoyer les lignes
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private void handleRead(Metadata metadata, ObjectOutputStream stream) throws ClassNotFoundException, IOException {
        String fileName = "./node-1/" + metadata.getFragmentName();
        Format reader = metadata.getFormat() == Type.KV ? new KVFormatS(fileName) : new LineFormatS(fileName);
        reader.open(Format.OpenMode.R);
        while (true) {
            KV record = (KV) reader.read();
            System.out.println(record);
            stream.writeObject(record);
            if (record == null) {
                break;
            }
        }
        reader.close();
    }

    /**
     * Traite une requête d'écriture.
     *
     * @param metadata Informations sur le fichier à écrire
     * @param stream   Flux lisible pour recevoir les lignes
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private void handleWrite(Metadata metadata, ObjectInputStream stream) throws ClassNotFoundException, IOException {
        String fileName = "./node-1/" + metadata.getFragmentName();
        Format writer = metadata.getFormat() == Type.KV ? new KVFormat(fileName) : new LineFormat(fileName);
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

    /**
     * Traite une requête de suppression.
     *
     * @param metadata Informations sur le fichier à supprimer
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private void handleDelete(Metadata metadata) throws ClassNotFoundException, IOException {
        // TODO
    }

    /**
     * Lance un serveur dans le dossier courant avec les paramètres par défaut.
     */
    public static void main(String[] args) throws IOException {
        new HdfsServer();
    }

}
