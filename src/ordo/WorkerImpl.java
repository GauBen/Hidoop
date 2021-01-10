package ordo;

import formats.Format;
import formats.Format.OpenMode;
import map.Mapper;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Deamon running on the server
 */
public class WorkerImpl extends UnicastRemoteObject implements Worker {

    private static final long serialVersionUID = 1L;

    public String id;

    /**
     * Settings for RMI
     */
    static String serverAddress = "//localhost";

    static int port = 1; // Offset du port

    public WorkerImpl(String hostDuRmi, int portDuRmi, String hostDistantDuNoeudHdfs, int portDuNodeHdfs)
            throws RemoteException {
        super();

        try {
            Naming.lookup(
                    hostDuRmi + ":" + portDuRmi + "/Node(" + hostDistantDuNoeudHdfs + " : " + portDuNodeHdfs + ")"); // TODO:
            // remplacer
            // la
            // convention
            // par
            // un
            // appel
            // de
            // fonction
            System.out.println(
                    "Attention, cet ID a déjà été enregistré dans le RMIRegistry. Est-ce un doublon ou un redemarrage ?");

        } catch (NotBoundException e) {
            // OK

        } catch (MalformedURLException | RemoteException e) {
            System.out.println("Le rmi registry n'est pas disponible sur " + Job.serverAddress + ":" + Job.port);
            System.exit(1);
        }

        try {
            Naming.rebind(
                    hostDuRmi + ":" + portDuRmi + "/Node(" + hostDistantDuNoeudHdfs + " : " + portDuNodeHdfs + ")",
                    this);
            System.out.println(
                    "Worker" + "/Node(" + hostDistantDuNoeudHdfs + " : " + portDuNodeHdfs + ")" + " bound in registry");

        } catch (RemoteException e) {
            System.out.println(
                    "Il semble que le rmiregistry ne contienne pas les classes nécessaires! Etes vous sur de l'avoir executé dans le bon dossier ?");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

    }

    public static void usage() {
        System.out.println("Usage : WorkerImpl <hostDuRmi> <portDuRmi> <hostDistantDuNoeudHdfs> <portDuNodeHdfs>");
        System.out.println("id >= 0");
    }

    public static void main(String[] args) {

        if (args.length != 4) {
            usage();
            return;
        }

        try {
            Integer.parseInt(args[1]);
            Integer.parseInt(args[3]);
            if (Integer.parseInt(args[0]) < 0) {
                usage();
                return;
            }
        } catch (NumberFormatException e) {
            usage();
            return;
        }

        String hostDuRmi = args[0];
        int portDuRmi = Integer.parseInt(args[1]);
        String hostDuNoeudHdfs = args[2];
        int portDuNoeudHdfs = Integer.parseInt(args[3]);

        WorkerImpl worker;
        try {
            worker = new WorkerImpl(hostDuRmi, portDuRmi, hostDuNoeudHdfs, portDuNoeudHdfs);
        } catch (RemoteException e1) {
            e1.printStackTrace();
        }

    }

    @Override
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {

        System.out.println("Voici le nom de mon fragment a ouvrir " + reader.getFname());

        Thread thread = new Thread() {
            public void run() {

                System.out.println("Creating a temporary file... " + writer.getFname());
                File tempResultFile = new File(writer.getFname());
                try {
                    tempResultFile.createNewFile();
                } catch (IOException e2) {
                    // TODO Auto-generated catch block
                    e2.printStackTrace();
                }


                File file = new File(reader.getFname());

                if (file.isFile() && !file.isDirectory()) {
                    //OK
                    reader.open(OpenMode.R);
                    writer.open(OpenMode.W);


                    m.map(reader, writer);

                    try {
                        try {
                            cb.done();
                        } catch (RemoteException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }


                } else { // The file to read doesn't exist on the node
                    try {
                        cb.error(id, "Fichier a lire non trouve !");
                    } catch (RemoteException e1) {
                        e1.printStackTrace();
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }


            }
        };

        thread.start();


    }

}
