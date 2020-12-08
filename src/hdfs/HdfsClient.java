/**
 * HDFS - Hidoop File System
 *
 * Client développé par Théo Petit et Gautier Ben Aïm
 */

package hdfs;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import formats.Format;
import formats.KV;
import formats.LineFormatS;

public class HdfsClient {

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

    public static void HdfsDelete(String hdfsFname) {
    }

    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {
        Format lf = new LineFormatS(localFSSourceFname);
        lf.open(Format.OpenMode.R);
        try {
            Socket sock = new Socket("127.0.0.1", 8123);
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
            File f = new File(localFSSourceFname);
            out.writeObject(new Metadata(f.getName()));
            while (true) {
                KV line = lf.read();
                System.out.println(line);
                out.writeObject(line);
                if (line == null) {
                    break;
                }
            }
            sock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
    }

    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length < 2) {
                usage();
                return;
            }

            switch (args[0]) {
                case "read":
                    HdfsRead(args[1], null);
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

}
