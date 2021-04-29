package ordo;

import map.FileLessMapperReducer;
import application.RmiCustomInterface;
import formats.Format;
import formats.Format.OpenMode;
import hdfs.HdfsNodeInfo;
import map.Mapper;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Deamon running on the server
 */
public class WorkerImpl extends UnicastRemoteObject implements Worker {

    private static final long serialVersionUID = 1L;

    public String id;

    public HdfsNodeInfo uri;

    public WorkerImpl(String hostDuRmi, int portDuRmi, String hostDistantDuNoeudHdfs, int portDuNodeHdfs)
            throws RemoteException, URISyntaxException {
        super();
        this.id = hostDistantDuNoeudHdfs + "/" + portDuNodeHdfs;


        this.uri = new HdfsNodeInfo(hostDistantDuNoeudHdfs, portDuNodeHdfs, "");

        try {

            Registry registry = LocateRegistry.getRegistry(hostDuRmi, portDuRmi);

            System.out.println("> Voici le contenu du registry distant");
            for (String element : registry.list()) {
                System.out.println(element);
            }

            Naming.lookup(workerAddress(hostDuRmi, portDuRmi, hostDistantDuNoeudHdfs, portDuNodeHdfs));

            System.out.println(
                    "Attention, cet ID a déjà été enregistré dans le RMIRegistry. Est-ce un doublon ou un redemarrage ?");
        } catch (NotBoundException e) {
            // OK
        } catch (RemoteException e) {
            System.out.println("Le rmi registry n'est pas disponible sur " + hostDuRmi + ":" + portDuRmi);
            System.exit(1);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            System.exit(2);
        }

        try {

            try {
                RmiCustomInterface rmi = (RmiCustomInterface) Naming.lookup("//" + hostDuRmi + ":" + portDuRmi + "/RMIMaster");

                //  Naming.rebind(workerAddress(hostDuRmi, portDuRmi, hostDistantDuNoeudHdfs, portDuNodeHdfs), this);
                rmi.registerNode(hostDuRmi, portDuRmi, hostDistantDuNoeudHdfs, portDuNodeHdfs, this);
                System.out.println("Worker " + hostDistantDuNoeudHdfs + ":" + portDuNodeHdfs + " enregistré");


            } catch (NotBoundException e) {
                e.printStackTrace();
            }


        } catch (RemoteException e) {
            System.out.println("Il est probable que le rmiregistry ne contienne pas les classes "
                    + "nécessaires! Etes vous sur de l'avoir executé dans le bon dossier ? Au cas ou, voici l'erreur : ");
            e.printStackTrace();
            System.exit(10);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

    }

    public static String workerAddress(String hostDuRmi, int portDuRmi, String hostDistantDuNoeudHdfs,
            int portDuNodeHdfs) {
        return "//" + hostDuRmi + ":" + portDuRmi + "/worker/" + hostDistantDuNoeudHdfs + "/" + portDuNodeHdfs;
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

        try {
            new WorkerImpl(hostDuRmi, portDuRmi, hostDuNoeudHdfs, portDuNoeudHdfs);
        } catch (RemoteException | URISyntaxException e1) {
            e1.printStackTrace();
        }

    }

    @Override
    public void runFileLessMap(FileLessMapperReducer m, HidoopTask task, Format writer, CallBack cb) throws RemoteException {

    }

    @Override
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {

        System.out.println("> Voici le nom de mon fragment a ouvrir " + reader.getFname());

        Thread thread = new Thread() {
            public void run() {

                long startTime = System.currentTimeMillis();
                System.out.println("> Creating a temporary file... " + writer.getFname());
                File tempResultFile = new File(writer.getFname());
                try {
                    tempResultFile.createNewFile();
                } catch (IOException e2) {
                    // TODO Auto-generated catch block
                    e2.printStackTrace();
                }

                // On recupere notre gragment a traiter
                File file = new File(reader.getFname());

                // Get the fragment ID
                int fragmentNumber = Integer.parseInt(reader.getFname().replaceAll("\\D", ""));

                if (file.isFile() && !file.isDirectory()) {
                    // OK
                    reader.open(OpenMode.R);
                    writer.open(OpenMode.W);

                    m.map(reader, writer);

                    try {
                        long endTime = System.currentTimeMillis();
                        cb.done(WorkerImpl.this.uri, endTime - startTime, fragmentNumber);
                    } catch (RemoteException | InterruptedException e) {
                        e.printStackTrace();
                    }

                } else { // The file to read doesn't exist on the node
                    try {
                        cb.error(id, "Fichier a lire non trouve !");
                    } catch (RemoteException | InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }

            }
        };

        thread.start();

    }

    @Override
    public int getNumberOfCores() throws RemoteException {
        return Runtime.getRuntime().availableProcessors();
    }

}
