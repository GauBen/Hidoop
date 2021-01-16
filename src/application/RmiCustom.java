package application;


import ordo.Worker;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;


public class RmiCustom extends UnicastRemoteObject implements RmiCustomInterface {
    protected RmiCustom() throws RemoteException {
    }

    static void usage() {
        System.out.println("RmiCustom : <port>");
    }

    public static void main(String[] args) throws RemoteException, MalformedURLException, InterruptedException {
        if (args.length != 1) {
            usage();
            return;
        }

        int portDuRmi = Integer.parseInt(args[0]);

        Registry registry = LocateRegistry.createRegistry(portDuRmi);

        registry.rebind("RMIMaster", new RmiCustom());
        System.out.println("Rmi demarre !");

        while (true) {

            System.out.println("------------");
            for (String element : registry.list()) {
                System.out.println(element);
            }
            Thread.sleep(10000);
        }

    }

    public void registerNode(String adresseDuRmi, int portDuRmi, String address, int port, Worker worker) throws MalformedURLException, RemoteException {
        String toRegister = workerAddress(adresseDuRmi, portDuRmi, address, port);

        Naming.rebind(toRegister, worker);
        System.out.println(toRegister + " s'est register !!!");
    }


    public String workerAddress(String adresseDuRmi, int portDuRmi, String hostDistantDuNoeudHdfs, int portDuNodeHdfs) {
        return "//" + adresseDuRmi + ":" + portDuRmi + "/worker/" + hostDistantDuNoeudHdfs + "/" + portDuNodeHdfs;
    }
}
