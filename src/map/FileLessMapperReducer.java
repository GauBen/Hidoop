package map;

import formats.FormatReader;
import formats.FormatWriter;
import ordo.HidoopTask;

/**
 * Pour les applications qui n'utilisent pas de fichier en input
 */
public interface FileLessMapperReducer extends MapReduce{
    public void map(HidoopTask input, FormatWriter writer);
}
