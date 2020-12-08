package ordo;

import formats.Format;
import formats.Format.OpenMode;
import map.Mapper;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Deamon running on the server
 */
public class WorkerImpl extends UnicastRemoteObject implements Worker {

    private static final long serialVersionUID = 1L;


    /**
     * Settings for RMI
     */
    static String serverAddress = "//localhost";


    protected WorkerImpl() throws RemoteException {
        super();
    }

    static int port = 4000;

    @Override
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
        // Open files
        reader.open(OpenMode.R);
        writer.open(OpenMode.W);


        m.map(reader, writer);

        cb.done();

    }


    public static void main(String[] args) {
        try {
            WorkerImpl worker = new WorkerImpl();
            String id = args[0]; // Id has to be a number between 0 and the total number of nodes minus 1

            Naming.rebind(WorkerImpl.serverAddress + ":" + WorkerImpl.port + "/Node" + id, worker);
            System.out.println("Worker" + args[0] + " bound in registry");

        } catch (RemoteException | MalformedURLException e) {
            e.printStackTrace();
        }
    }

}