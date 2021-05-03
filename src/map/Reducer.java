package map;

import formats.FormatReader;
import formats.FormatWriter;

import java.io.Serializable;

public interface Reducer extends Serializable {
    void reduce(FormatReader reader, FormatWriter writer);
}
