package ordo;

import java.util.List;

/**
 * Class used when a Job has no input file : it replaces Fragments
 */
public class HidoopTask implements SerializableTask {
    public String nom;

    public List<Integer> dataEntier;
    public String dataString;

    public HidoopTask(String nom, List<Integer> dataEntier, String dataString) {
        this.nom = nom;
        this.dataEntier = dataEntier;
        this.dataString = dataString;
    }
}
