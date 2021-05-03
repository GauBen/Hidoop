package application;

import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.FileLessMapperReducer;
import ordo.HidoopTask;
import ordo.Job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EvaluationPageRank implements FileLessMapperReducer {

    private static final long serialVersionUID = 1L;

    // Coefficient s pour le calcul du PR
    public static double s = 0.85;

    //public int offset;

    // Nombre de pages du site permettant de calculer E(page)
    public int nbPages;

    public static int iteration;

    public boolean finIterations;

    Map<String, PaireGraphePR> liensDePages;

    public EvaluationPageRank(Map<String,PaireGraphePR> liensDePages){
        //this.offset = offset;

        this.liensDePages = liensDePages;

        this.nbPages = this.liensDePages.size();

        this.finIterations = false;
    }

    @Override
    public void map(FormatReader reader, FormatWriter writer) {
    }


    @Override
    public void map(HidoopTask input, FormatWriter writer) {

        int offset = input.dataEntier.get(0);

        List<String> keys = new ArrayList<String>(this.liensDePages.keySet());
        String pageCourante = keys.get(offset);

        PaireGraphePR infosPageCourante = this.liensDePages.get(pageCourante);
        double prPageCourante = infosPageCourante.pr;
        int nbLiensPageCourante = infosPageCourante.liens.size();

        for (String lien : infosPageCourante.liens) {
            // On écrit à la fois le PR et le nombre de liens de la page courante
            writer.write(new KV(lien, prPageCourante + "-" + nbLiensPageCourante));
        }

        if (infosPageCourante.liens.isEmpty()){
            writer.write(new KV("rien", "rien"));
        }
    }

    @Override
    public void reduce(FormatReader reader, FormatWriter writer) {
        HashMap<String, Double> newPR = new HashMap<>();

        // On initialise les nouveaux PR avec (1-s) * E(pi)
        for (Map.Entry<String, PaireGraphePR> entree : this.liensDePages.entrySet()) {
		    newPR.put(entree.getKey(), (1 - EvaluationPageRank.s) / this.nbPages);
        }
        KV kv;
        while ((kv = reader.read()) != null) {
            if (this.liensDePages.containsKey(kv.k)) {
                String[] parts = kv.v.split("-");
                String pr = parts[0];
                String nbLiens = parts[1];

            	// On ajoute la valeur PR(pj) / nbLiens(pj)
                newPR.put(kv.k, newPR.get(kv.k) + EvaluationPageRank.s * (Double.parseDouble(pr) / Integer.parseInt(nbLiens)));
            } else if (kv.k.equals("rien")){
            }
            else
                throw new RuntimeException();
        }

        for (Map.Entry<String, Double> entree : newPR.entrySet()) {
            System.out.println(entree.getKey()+"<->"+entree.getValue()+";");
            System.out.println();
        }

        boolean tousPRSousSeuil = true;
        for (Map.Entry<String, PaireGraphePR> entree : this.liensDePages.entrySet()) {
            double prCourant = entree.getValue().pr;

            // On vérifie si le nouveau PR est très proche de l'ancien
            double nouveauPRCourant = newPR.get(entree.getKey());
            tousPRSousSeuil = tousPRSousSeuil && Math.abs(prCourant - nouveauPRCourant) < 1e-7;

            // On met à jour le PR de la page courante
            this.liensDePages.put(entree.getKey(), new PaireGraphePR(nouveauPRCourant, entree.getValue().liens));
        }

        // On regarde s'il faut arreter l'algorithme (convergence ou nombre d'itérations est dépassé)
        // La valeur de 100 itérations est fixée arbitrairement
        this.finIterations = tousPRSousSeuil || EvaluationPageRank.iteration > 100;

        if (this.finIterations){
            for (Map.Entry<String, PaireGraphePR> entree : this.liensDePages.entrySet()) {
                writer.write(new KV(entree.getKey(), String.valueOf(entree.getValue().pr)));
            }
        } else {
            EvaluationPageRank.iteration++;
        }
    }

    public static void usage() {
        System.out.println("Usage : EvaluationPageRank <URL>");
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            usage();
            return;
        }

        long t1 = System.currentTimeMillis();

        CGSeq.main(args);
        // TODO : faire un startJob dans le CG pour effectuer le travail en parallèle

        EvaluationPageRank epr = new EvaluationPageRank(CGSeq.liensDePages);

        List<HidoopTask> taches = new ArrayList<>();
        int offset = 0;
        for (int i = 0; i < epr.nbPages; i++){
            List<Integer> argumentsTache = new ArrayList<>();
            argumentsTache.add(offset);
            taches.add(new HidoopTask("resultat_pageRank.txt", argumentsTache, ""));

            offset += 1;
        }
        Job j = Job.FileLessJob(taches);

        while (!epr.finIterations){
        	j.startJob(epr);
        }

        long t2 = System.currentTimeMillis();
        System.out.println("time in ms =" + (t2 - t1));
        System.exit(0);
    }
}
