package application;


import ordo.Worker;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public class RmiCustom implements Remote {

    private int portDuRmi;

    static void usage() {
        System.out.println("RmiCustom : <port>");
    }

    public void main(String[] args) throws RemoteException, MalformedURLException {
        if (args.length != 1) {
            usage();
        }

        portDuRmi = Integer.parseInt(args[0]);


        LocateRegistry.createRegistry(portDuRmi);

        Naming.rebind("//localhost:" + portDuRmi + "/RMIMaster", this);

    }

    public void registerNode(String address, int port, Worker worker) throws MalformedURLException, RemoteException {
        String toRegister = workerAddress(address, port);

        Naming.rebind(toRegister, worker);
        System.out.println(toRegister + " s'est register !!!");
    }


    public String workerAddress(String hostDistantDuNoeudHdfs, int portDuNodeHdfs) {
        return "//" + "localhost" + ":" + portDuRmi + "/worker/" + hostDistantDuNoeudHdfs + "/" + portDuNodeHdfs;
    }

}
