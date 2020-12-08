package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

import formats.Format;
import formats.KV;
import formats.KVFormat;

public class HdfsServer {

    private ServerSocket server;

    public HdfsServer() {
        int port = 8123;
        try {
            this.server = new ServerSocket(port);
            System.out.println("Serveur lancé sur le port " + this.server.getLocalPort() + ".");
            System.out.println("Ctrl+C pour arrêter le serveur.");
            this.run();
        } catch (IOException e) {
            System.out.println("Impossible de lancer le serveur, le port est peut-être occupé.");
            e.printStackTrace();
        }
    }

    private void run() {
        while (true) {
            Socket sock = null;
            try {
                sock = this.server.accept();
                ObjectInputStream stream = new ObjectInputStream(sock.getInputStream());
                Metadata metadata = (Metadata) stream.readObject();
                System.out.println(metadata);
                Format lf = new KVFormat("./node-1/" + metadata.getName() + ".rmlkk");
                lf.open(Format.OpenMode.W);
                while (true) {
                    KV kv = (KV) stream.readObject();
                    if (kv == null)
                        break;
                    lf.write(kv);
                }
                sock.close();
            } catch (IOException e) {
                if (sock != null) {
                    System.out.println("Socket " + sock.getInetAddress() + " interrompue.");
                } else {
                    System.out.println("Une connexion a échoué.");
                }
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        new HdfsServer();
    }

}
