package hdfs;

import java.io.Serializable;

public class HdfsNodeInfo implements Serializable {

    private static final long serialVersionUID = 2248373901427128684L;

    private String host;
    private int port;
    private String root;

    public HdfsNodeInfo(String host, int port, String root) {
        this.host = host;
        this.port = port;
        this.root = root;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getRoot() {
        return root;
    }

    public boolean matches(String host, int port) {
        return this.host.equals(host) && this.port == port;
    }

    public boolean matches(HdfsNodeInfo node) {
        return this.matches(node.getHost(), node.getPort());
    }

    public String toString() {
        return getHost() + ":" + getPort();
    }

    public int hashCode() {
        return toString().hashCode();
    }

}
