package hdfs;

import java.io.Serializable;

public class Metadata implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;

    public Metadata(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

}
