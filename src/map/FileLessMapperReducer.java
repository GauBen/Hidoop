package map;

import formats.FormatWriter;
import ordo.HidoopTask;

/**
 * Pour les applications qui n'utilisent pas de fichier en input
 */
public interface FileLessMapperReducer extends MapReduce{
    void map(HidoopTask input, FormatWriter writer);
}
