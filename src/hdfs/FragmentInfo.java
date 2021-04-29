package hdfs;

import java.io.File;
import java.io.Serializable;

public class FragmentInfo implements Serializable {
    private static final long serialVersionUID = -1636990109710437159L;
    public String filename;
    public int id;
    public boolean lastPart;
    public HdfsNodeInfo node;
    public String root;

    public FragmentInfo(String filename, int id, boolean lastPart, HdfsNodeInfo node, String root) {
        this.filename = filename;
        this.id = id;
        this.lastPart = lastPart;
        this.node = node;
        this.root = root;
    }

    public String getFragmentName() {
        return filename + "." + id + (lastPart ? ".final" : "") + ".part";
    }

    public String getAbsolutePath() {
        return new File(this.root, this.getFragmentName()).getAbsolutePath();
    }

    public static String makeFragmentName(String filename, int id, boolean lastPart) {
        return new FragmentInfo(filename, id, lastPart, null, null).getFragmentName();
    }
}
