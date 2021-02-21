package hdfs;

import java.io.Serializable;

import formats.Format.Type;

public class Metadata implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;
    private Type format;

    public Metadata(String name, Type format) {
        this.name = name;
        this.format = format;
    }

    public String getName() {
        return this.name;
    }

    public Type getFormat() {
        return this.format;
    }

}
