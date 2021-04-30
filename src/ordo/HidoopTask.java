package ordo;

import java.util.List;

public class HidoopTask {
    public String nom;

    public List<Integer> dataEntier;
    public String dataString;

    public HidoopTask(String nom, List<Integer> dataEntier, String dataString) {
        this.nom = nom;
        this.dataEntier = dataEntier;
        this.dataString = dataString;
    }
}
