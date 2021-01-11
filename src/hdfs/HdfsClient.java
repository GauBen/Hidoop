/**
 * HDFS - Hidoop Distributed File System
 *
 * Client développé par Théo Petit et Gautier Ben Aïm
 */

package hdfs;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import formats.Format;
import formats.KV;
import formats.KVFormatS;
import formats.LineFormatS;
import formats.Format.Type;
import hdfs.HdfsNameServer.Action;
import hdfs.HdfsNameServer.FragmentInfo;

/**
 * Un client HDFS, qui distribue des fragments de fichiers aux noeuds HDFS.
 */
public class HdfsClient {

    /**
     * Nom des fragements sauvegardés sur les noeuds.
     *
     * @param fileName Nom du fichier
     * @return
     */
    @Deprecated
    public static String getFragmentName(String fileName) {
        return new File(fileName).getName() + ".part";
    }

    /**
     * Lecture et reconstitution d'un fichier sauvegardé sur les noeuds.
     *
     * @param hdfsFname        Nom du fichier distant
     * @param localFSDestFname Nom du fichier local dans lequel écrire
     */
    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
        // On ouvre le fichier local à écrire
        Format lf = null;
        if (localFSDestFname != null) {
            lf = new LineFormatS(localFSDestFname);
            lf.open(Format.OpenMode.W);
        }

        try {

            // Connexion au premier noeud
            Socket sock = newNameServerSocket();
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());

            // On lui envoie que l'on veut lire un fichier
            out.writeObject(Action.READ);
            File f = new File(hdfsFname);
            out.writeObject(new Metadata(f.getName(), Type.LINE));

            // On réceptionne toutes les lignes
            while (true) {
                KV line = (KV) in.readObject();
                if (line == null) {
                    break;
                }
                if (lf != null) {
                    lf.write(line);
                } else {
                    System.out.println(line);
                }
            }

            sock.close();
            if (lf != null) {
                lf.close();
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
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
        // On ouvre le fichier à envoyer
        Format lf = fmt == Type.KV ? new KVFormatS(localFSSourceFname) : new LineFormatS(localFSSourceFname);
        lf.open(Format.OpenMode.R);

        try {

            // Connexion au premier noeud
            Socket sock = newNameServerSocket();
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());

            // On l'informe qu'on veut écrire un fichier
            out.writeObject(Action.WRITE);
            File f = new File(localFSSourceFname);
            out.writeObject(new Metadata(f.getName(), fmt));

            // On envoie le fichier
            while (true) {
                KV line = lf.read();
                out.writeObject(line);
                if (line == null) {
                    break;
                }
            }

            Object response = new ObjectInputStream(sock.getInputStream()).readObject();
            assert response == Action.PONG;
            sock.close();

        } catch (IOException | ClassNotFoundException e) {
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
            out.writeObject(Action.DELETE);
            out.writeObject(hdfsFname);

            Object response = new ObjectInputStream(sock.getInputStream()).readObject();
            assert response == Action.PONG;
            sock.close();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static List<FragmentInfo> listFragments(String hdfsFilename) {
        try {
            Socket sock = newNameServerSocket();
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());

            // On l'informe qu'on veut la liste des fragments
            out.writeObject(Action.LIST_FRAGMENTS);
            out.writeObject(hdfsFilename);

            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());

            List<FragmentInfo> lst = new ArrayList<>();
            for (Object i : (List<?>) in.readObject()) {
                lst.add((FragmentInfo) i);
            }

            out.writeObject(Action.PONG);
            return lst;

        } catch (IOException | ClassNotFoundException e) {
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
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());

            // On force le rafraîchissement du catalogue
            out.writeObject(Action.FORCE_RESCAN);

            assert Action.PONG == new ObjectInputStream(in).readObject();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * @return Une socket ouverte sur le NameServer HDFS
     */
    private static Socket newNameServerSocket() throws UnknownHostException, IOException {
        return new Socket("127.0.0.1", HdfsNameServer.DEFAULT_PORT);
    }

    /**
     * Interface en ligne de commande pour HDFS.
     *
     * @param args Arguments passés au programme
     */
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length < 2) {
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
                    Format.Type fmt;
                    if (args.length < 3) {
                        usage();
                        return;
                    }
                    if (args[1].equals("line"))
                        fmt = Format.Type.LINE;
                    else if (args[1].equals("kv"))
                        fmt = Format.Type.KV;
                    else {
                        usage();
                        return;
                    }
                    HdfsWrite(fmt, args[2], 1);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Affiche une aide textuelle.
     */
    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file> [<dest> optional]");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

}
