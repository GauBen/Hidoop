/**
 * HDFS - Hidoop Distributed File System
 *
 * Client développé par Théo Petit et Gautier Ben Aïm
 */

package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
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

        try (BufferedOutputStream file = new BufferedOutputStream(new FileOutputStream(localFSDestFname))) {

            // Connexion au premier noeud
            Socket sock = newNameServerSocket();
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());

            // On lui envoie que l'on veut lire un fichier
            File f = new File(hdfsFname);
            out.writeObject(HdfsAction.READ);
            out.writeObject(f.getName());

            ObjectInputStream serverInputStream = new ObjectInputStream(new BufferedInputStream(sock.getInputStream()));

            // Réception des erreurs
            HdfsRuntimeException exception = (HdfsRuntimeException) serverInputStream.readObject();
            if (exception != null) {
                throw exception;
            }

            int number_of_fragments = serverInputStream.readInt();
            int size = serverInputStream.readInt();
            ByteArrayInputStream buffer = null;

            int i = 1;

            while (size > 0) {
                System.out.print("\r" + i + "/" + number_of_fragments + " fragments");
                buffer = new ByteArrayInputStream(serverInputStream.readNBytes(size));
                buffer.transferTo(file);
                size = serverInputStream.readInt();
                i++;
            }

            System.out.println();

            if (size < 0) {
                throw new HdfsRuntimeException("Le fichier ne peut pas être téléchargé en entier");
            }

            out.writeObject(HdfsAction.PONG);
            sock.close();

        } catch (IOException e) {
            System.err.println("La lecture a échoué, suppression du fichier local.");
            try {
                Files.delete(Path.of(localFSDestFname));
            } catch (IOException e2) {
            }
            throw new HdfsRuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new HdfsRuntimeException(e);
        } catch (HdfsRuntimeException e) {
            System.err.println("Erreur reçue : " + e.getMessage());
            try {
                Files.delete(Path.of(localFSDestFname));
            } catch (IOException e2) {
            }
            throw e;
        }

    }

    /**
     * Écriture d'un fichier local vers les noeuds HDFS, après avoir été fragmenté.
     *
     * @param fmt                Ignoré, conservé pour rétro-compatibilité
     * @param localFSSourceFname Fichier local
     * @param repFactor          Facteur de duplication
     */
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {
        try {
            // Connexion au serveur de nom
            Socket sock = newNameServerSocket();
            ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(sock.getOutputStream()));

            // On l'informe qu'on veut écrire un fichier
            File f = new File(localFSSourceFname);
            out.writeObject(HdfsAction.WRITE);
            out.writeObject(f.getName());
            out.writeInt(repFactor);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(sock.getInputStream()));
            HdfsRuntimeException exception = (HdfsRuntimeException) in.readObject();

            if (exception != null) {
                throw exception;
            }

            // On envoie le fichier
            Files.copy(f.toPath(), out);
            out.flush();
            sock.shutdownOutput();

            Object response = in.readObject();
            if (response != HdfsAction.PONG) {
                throw new HdfsRuntimeException("Le serveur a rencontré une erreur lors du téléchargement");
            }
            sock.close();

        } catch (IOException | ClassNotFoundException e) {
            throw new HdfsRuntimeException(e);
        } catch (HdfsRuntimeException e) {
            System.err.println("Erreur reçue : " + e.getMessage());
            throw e;
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
            if (response != HdfsAction.PONG) {
                throw new HdfsRuntimeException("Le serveur a rencontré une erreur lors de la suppression");
            }
            sock.close();

        } catch (IOException | ClassNotFoundException e) {
            throw new HdfsRuntimeException(e);
        }
    }

    /**
     * Demande la liste des fragments d'un fichier distant.
     *
     * Remarque : un fichier peut être répliqué, d'où List<List<FragmentInfo>>
     *
     * @param hdfsFilename
     * @return
     */
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
            throw new HdfsRuntimeException(e);
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

            Object response = new ObjectInputStream(sock.getInputStream()).readObject();
            if (response != HdfsAction.PONG) {
                throw new HdfsRuntimeException("Le serveur a rencontré une erreur lors du rafraîchissement");
            }

        } catch (IOException | ClassNotFoundException e) {
            throw new HdfsRuntimeException(e);
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
            System.out.println("Téléchargement réalisé avec succès");
            break;
        case "delete":
            HdfsDelete(args[1]);
            System.out.println("Suppresion réalisée avec succès");
            break;
        case "write":
            if (args.length < 2) {
                usage();
                return;
            }
            HdfsWrite(Format.Type.KV, args[1], args.length < 3 ? 1 : Integer.parseInt(args[2]));
            System.out.println("Upload réalisé avec succès");
        }
    }

    /**
     * Affiche une aide textuelle.
     */
    private static void usage() {
        System.out.println("Usage:");
        System.out.println("  * HdfsClient read <file> <dest>");
        System.out.println("  * HdfsClient write <file> <rep? = 1>");
        System.out.println("  * HdfsClient delete <file>");
        System.out.println("  * HdfsClient rescan");
    }

}
