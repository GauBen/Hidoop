// une *proposition*, qui  peut être complétée, élaguée ou adaptée

package ordo;

import formats.Format;

public interface JobInterfaceX extends JobInterface {
    int getNumberOfReduces();

    void setNumberOfReduces(int tasks);

    int getNumberOfMaps();

    void setNumberOfMaps(int tasks);

    Format.Type getInputFormat();

    Format.Type getOutputFormat();

    void setOutputFormat(Format.Type ft);

    String getInputFname();

    String getOutputFname();

    void setOutputFname(String fname);

    SortComparator getSortComparator();

    void setSortComparator(SortComparator sc);
}
