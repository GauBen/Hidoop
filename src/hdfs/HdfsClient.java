/**
 * HDFS - Hidoop Distributed File System
 *
 * Client développé par Théo Petit et Gautier Ben Aïm
 */

package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import formats.Format;

/**
 * Un client HDFS, qui distribue des fragments de fichiers aux noeuds HDFS.
 */
public class HdfsClient {

    /**
     * Lecture et reconstitution d'un fichier sauvegardé sur les noeuds.
     *
     * @param hdfsFname        Nom du fichier distant
     * @param localFSDestFname Nom du fichier local dans lequel écrire
     */
    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
        Objects.requireNonNull(localFSDestFname);

        try {

            // Connexion au premier noeud
            Socket sock = newNameServerSocket();
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());

            // On lui envoie que l'on veut lire un fichier
            File f = new File(hdfsFname);
            out.writeObject(HdfsAction.READ);
            out.writeObject(f.getName());

            BufferedInputStream in = new BufferedInputStream(sock.getInputStream());
            ObjectInputStream serverInputStream = new ObjectInputStream(in);

            // Réception des erreurs
            HdfsRuntimeException exception = (HdfsRuntimeException) serverInputStream.readObject();
            if (exception != null) {
                throw exception;
            }

            Files.copy(in, Path.of(localFSDestFname), StandardCopyOption.REPLACE_EXISTING);

            out.writeObject(HdfsAction.PONG);
            sock.close();

        } catch (IOException e) {
            System.err.println("La lecture a échoué.");
            try {
                Files.delete(Path.of(localFSDestFname));
            } catch (IOException e2) {
            }
        } catch (ClassNotFoundException e) {
        } catch (HdfsRuntimeException e) {
            System.err.println("Erreur reçue : " + e.getMessage());
        }

    }

    /**
     * Écriture d'un fichier local vers les noeuds HDFS, après avoir été fragmenté.
     *
     * @param fmt                Le format du fichier (Line ou KV)
     * @param localFSSourceFname Fichier local
     * @param repFactor          Facteur de duplication (ignoré, toujours 1)
     */
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {
        try {
            // Connexion au serveur de nom
            Socket sock = newNameServerSocket();
            BufferedOutputStream rawOut = new BufferedOutputStream(sock.getOutputStream());
            ObjectOutputStream out = new ObjectOutputStream(rawOut);

            // On l'informe qu'on veut écrire un fichier
            File f = new File(localFSSourceFname);
            out.writeObject(HdfsAction.WRITE);
            out.writeObject(f.getName());

            // On envoie le fichier
            Files.copy(f.toPath(), rawOut);
            rawOut.flush();
            sock.shutdownOutput();

            Object response = new ObjectInputStream(sock.getInputStream()).readObject();
            // TODO Gestion du pong
            assert response == HdfsAction.PONG;
            sock.close();

        } catch (IOException | ClassNotFoundException e) {
            // TODO Gestion de l'erreur d'écriture
            e.printStackTrace();
        }
    }

    /**
     * Demande la suppression d'un fichier distant.
     *
     * @param hdfsFname
     */
    public static void HdfsDelete(String hdfsFname) {
        try {
            // Connexion au premier noeud
            Socket sock = newNameServerSocket();
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());

            // On l'informe qu'on veut supprimer un fichier
            out.writeObject(HdfsAction.DELETE);
            out.writeObject(hdfsFname);

            Object response = new ObjectInputStream(sock.getInputStream()).readObject();
            // TODO Gestion du pong
            assert response == HdfsAction.PONG;
            sock.close();

        } catch (IOException | ClassNotFoundException e) {
            // TODO Gestion de l'erreur de suppression
            e.printStackTrace();
        }
    }

    public static List<List<FragmentInfo>> listFragments(String hdfsFilename) {
        try {
            Socket sock = newNameServerSocket();
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());

            // On l'informe qu'on veut la liste des fragments
            out.writeObject(HdfsAction.LIST_FRAGMENTS);
            out.writeObject(hdfsFilename);

            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());

            List<List<FragmentInfo>> lst = new ArrayList<>();
            for (Object i : (List<?>) in.readObject()) {
                Objects.requireNonNull(i);
                lst.add(((List<?>) i).stream().map(obj -> (FragmentInfo) obj).collect(Collectors.toList()));
            }

            out.writeObject(HdfsAction.PONG);
            return Collections.unmodifiableList(lst);

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Une erreur de connexion a eu lieu lors de la récupération des fragments.");
            throw new HdfsRuntimeException(e);
        }
    }

    public static Set<HdfsNodeInfo> listNodes() {
        try {
            Socket sock = newNameServerSocket();
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());

            // On l'informe qu'on veut la liste des fragments
            out.writeObject(HdfsAction.LIST_NODES);

            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());

            Set<HdfsNodeInfo> set = new HashSet<>();
            for (Object i : (Set<?>) in.readObject()) {
                Objects.requireNonNull(i);
                set.add((HdfsNodeInfo) i);
            }

            out.writeObject(HdfsAction.PONG);
            return Collections.unmodifiableSet(set);

        } catch (IOException | ClassNotFoundException e) {
            // TODO Gestion de l'erreur de récupération de la liste
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Provoque un rafraichissement de la liste des fichiers.
     */
    public static void requestRefresh() {
        try {
            Socket sock = newNameServerSocket();

            // On force le rafraîchissement du catalogue
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
            out.writeObject(HdfsAction.FORCE_RESCAN);

            // TODO Gestion du pong
            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
            assert HdfsAction.PONG == new ObjectInputStream(in).readObject();

        } catch (IOException | ClassNotFoundException e) {
            // TODO Gestion de l'erreur du rafraichissement
            e.printStackTrace();
        }
    }

    /**
     * @return Une socket ouverte sur le NameServer HDFS
     */
    private static Socket newNameServerSocket() throws UnknownHostException, IOException {
        return new Socket(HdfsNameServer.DEFAULT_HOST, HdfsNameServer.DEFAULT_PORT);
    }

    /**
     * Interface en ligne de commande pour HDFS.
     *
     * @param args Arguments passés au programme
     */
    public static void main(String[] args) {
        System.out.println("=== HDFS Client ===");
        if (args.length == 0) {
            usage();
            return;
        }

        switch (args[0]) {
        case "rescan":
            requestRefresh();
            break;
        case "read":
            HdfsRead(args[1], args.length < 3 ? null : args[2]);
            break;
        case "delete":
            HdfsDelete(args[1]);
            break;
        case "write":
            if (args.length < 2) {
                usage();
                return;
            }
            HdfsWrite(Format.Type.KV, args[1], 1);
        }
    }

    /**
     * Affiche une aide textuelle.
     */
    private static void usage() {
        System.out.println("Usage:");
        System.out.println("  * HdfsClient read <file> <dest>");
        System.out.println("  * HdfsClient write <file>");
        System.out.println("  * HdfsClient delete <file>");
        System.out.println("  * HdfsClient rescan");
    }

}
