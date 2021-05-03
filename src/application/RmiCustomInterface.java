package application;

import ordo.Worker;

import java.net.MalformedURLException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RmiCustomInterface extends Remote {

    void registerNode(String adresseDuRmi, int portDuRmi, String address, int port, Worker worker) throws MalformedURLException, RemoteException;

}
