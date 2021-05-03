package map;

import formats.FormatReader;
import formats.FormatWriter;

import java.io.Serializable;

public interface Mapper extends Serializable {
    void map(FormatReader reader, FormatWriter writer);
}
