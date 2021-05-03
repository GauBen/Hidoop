package formats;

import java.io.Serializable;

public interface Format extends FormatReader, FormatWriter, Serializable {
    void open(OpenMode mode);

    void close();

    long getIndex();

    String getFname();

    void setFname(String fname);

    enum Type {
        LINE, KV
    }

    enum OpenMode {
        R, W
    }

}
