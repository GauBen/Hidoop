package application.pageRanking;

import java.io.Serializable;
import java.util.Set;

public class PaireGraphePR implements Serializable {

    private static final long serialVersionUID = 1L;

    public double pr;
    public Set<String> liens;

    public PaireGraphePR() {
    }

    public PaireGraphePR(double pr, Set<String> liens) {
        super();
        this.pr = pr;
        this.liens = liens;
    }

}
