package ordo;

import formats.Format;
import map.MapReduce;

public interface JobInterface {
    // Méthodes requises pour la classe Job
    void setInputFormat(Format.Type ft);

    void setInputFname(String fname);

    void startJob(MapReduce mr);
}
