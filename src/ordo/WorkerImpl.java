package ordo;

import formats.Format;
import formats.Format.OpenMode;
import map.Mapper;

import java.io.File;
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

    protected WorkerImpl() throws RemoteException {
        super();

    }

    static int port = 4000;

    public static void usage() {
        System.out.println("Usage : WorkerImpl <id>");
        System.out.println("id >= 0");
    }

    public static void main(String[] args) {
        try {
            if (args.length < 1) {
                usage();
                return;
            }
            WorkerImpl worker = new WorkerImpl();

            try {
                Integer.parseInt(args[0]);
                if (Integer.parseInt(args[0]) < 0) {
                    usage();
                    return;
                }
            } catch (NumberFormatException e) {
                usage();
                return;
            }

            worker.id = args[0]; // Id has to be a number between 0 and the total number of nodes minus 1


            try {


                Naming.lookup(WorkerImpl.serverAddress + ":" + WorkerImpl.port + "/Node" + worker.id);
                System.out.println("Attention, cet ID a déjà été enregistré dans le RMIRegistry. Est-ce un doublon ou un redemarrage ?");

            } catch (NotBoundException e) {
                // OK

            } catch (MalformedURLException | RemoteException e) {
                System.out.println("Le rmi registry n'est pas disponible sur " + Job.serverAddress + ":" + Job.port);
                System.exit(1);
            }

            Naming.rebind(WorkerImpl.serverAddress + ":" + WorkerImpl.port + "/Node" + worker.id, worker);
            System.out.println("Worker" + args[0] + " bound in registry");

            /* Tentative de unbind lorsque le node s'arrête
            PrintStream stream = System.out;

            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run() {
                    super.run();
                    try {
                        stream.println("Let's unbind!");
                        Naming.unbind(WorkerImpl.serverAddress + ":" + WorkerImpl.port + "/Node" + worker.id);
                    } catch (RemoteException | NotBoundException | MalformedURLException e) {
                        e.printStackTrace();
                    }
                }
            });
        */
        } catch (RemoteException e) {
            System.out.println("Il semble que le rmiregistry ne contienne pas les classes nécessaires! Etes vous sur de l'avoir executé dans le bon dossier ?");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {

        System.out.println("Voici le nom de mon fragment a ouvrir " + reader.getFname());

        Thread thread = new Thread() {
            public void run() {


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
