package application;// v0.0 PM, le 18/12/17

import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.FileLessMapperReducer;
import ordo.HidoopTask;
import ordo.Job;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;


public class CG implements FileLessMapperReducer {
    /* Construction du Graphe : produit un fichier de paires URL <-> PR;liste URL_liens
     * les liens de la liste étant séparés par des espaces
     */
    static Map<String, PaireGraphePR> liensDePages;
    String site;

    public List<String> linksString;
    public String page;
    public Set<String> liens;

    public CG() {
        CG.liensDePages = new HashMap<String,PaireGraphePR>();
    }

    void produire(Map<String, PaireGraphePR> pages) {
        //affiche les paires sur la sortie standard (rediriger si besoin)
        for (Map.Entry<String, PaireGraphePR> entree : liensDePages.entrySet()) {
            System.out.print(entree.getKey()+"<->"+entree.getValue().pr+";");
            for (String url : entree.getValue().liens) {
                System.out.print(url+" ");
            }
            System.out.println();
        }
    }

    boolean horsSite(String url) {
        //vrai si l'url n'est pas préfixée par le nom du site
        try {
            return !(new URL(url).getHost().equals(site));
        }
        catch (MalformedURLException mu) {
            System.out.println("URL incorrecte :  "+mu);
            return true;
        }

    }

    @Override
    public void map(HidoopTask input, FormatWriter writer) {
        int offset = input.dataEntier.get(0);

        String lien = this.linksString.get(offset);

        try {
            //les liens ajoutés doivent être valides
            Jsoup.connect(lien).get();
            //et sur le site
            if (! this.horsSite(lien)) {
                // On n'a pas de doublon (set)
                writer.write(new KV(this.page, lien));
            }
        }
        catch (IOException iox) {
            System.out.println("lien inaccessible "+lien+" "+iox);
        }

        writer.write(new KV("rien", "rien"));
    }

    @Override
    public void map(FormatReader reader, FormatWriter writer) {

    }

    @Override
    public void reduce(FormatReader reader, FormatWriter writer) {
        KV kv;
        while ((kv = reader.read()) != null) {
            if (kv.k.equals(this.page)) {
                String lien = kv.v;
                this.liens.add(lien);
            } else if (kv.k.equals("rien")){

            }
            else
                throw new RuntimeException();
        }

        this.liens.remove(this.page); // suppression auto référence éventuelle

        System.out.println("nb liens : "+liens.size());

        // On ajoute dans notre table les liens associés à la page en cours de traitement
        CG.liensDePages.put(this.page, new PaireGraphePR(1.0, new HashSet<String>()));
        CG.liensDePages.get(this.page).liens.addAll(liens);

        writer.write(new KV(this.page, ""));

    }

    public static void main(String[] args) {
        // 1 argument : URL du site à évaluer
        CG coGr = null;
        String page;
        String lien;
        URL u;
        Document doc;
        Set<String> liens = new HashSet<String>();
        LinkedList<String> aTraiter = new LinkedList<String>();

        if (args.length == 1) {
            coGr = new CG();
            aTraiter.add(args[0]);
            try {
                URL usite = new URL(args[0]);
                coGr.site = usite.getHost();
            }
            catch (MalformedURLException mu) {
                System.out.println("Argument attendu :  URL du site à évaluer "+mu);
                System.exit (1);
            }
        } else {
            System.out.println("Nb d'arguments ≠ 1. "+
                               "Un seul argument est attendu : URL du site à évaluer");
            System.exit (1);
        }
        coGr.liens = new HashSet<String>();
        while (aTraiter.size()>0) {

            // On vide la HashSet liens
            coGr.liens.clear();

            // On prend le premier élément de aTraiter et on le retire
            coGr.page = aTraiter.pollFirst();

            //System.out.println("page : "+page);
            try {
                // On se connecte au document et on ne garde que les liens
                doc = Jsoup.connect(coGr.page).get();

                Elements links = doc.select("a[href]");

                coGr.linksString = new ArrayList<>();
                for (Element link : links) {
                    try {
                        // On prend l'url associée à la href
                        u = new URL(link.attr("abs:href"));
                        // élimination des références
                        coGr.linksString.add(u.getProtocol() + "://" + u.getAuthority() + u.getFile());
                    } catch (MalformedURLException mu) {
                        System.out.println("lien erroné " + link + " " + mu);
                    }
                }


                List<HidoopTask> taches = new ArrayList<>();
                int offset = 0;

                if (!coGr.linksString.isEmpty()) {
                    for (int i = 0; i < coGr.linksString.size(); i++) {
                        List<Integer> argumentsTache = new ArrayList<>();
                        argumentsTache.add(offset);
                        taches.add(new HidoopTask("resultat_cg.txt", argumentsTache, ""));
                        offset += 1;
                    }

                    Job j = Job.FileLessJob(taches);

                    j.startJob(coGr);

                    //ajouter les liens à traiter trouvés dans la page courante
                    for (String url : coGr.liens) {
                        if (!(CG.liensDePages.containsKey(url) || aTraiter.contains(url))) {
                            aTraiter.add(url);
                        }
                    }
                } else {
                    CG.liensDePages.put(coGr.page, new PaireGraphePR(1.0, new HashSet<String>()));
                }
            }

            catch (IOException e) { //levée par doc = Jsoup.connect(page).get();
                System.out.println("erreur chargement "+coGr.page+" ("+e+")");
            }
        }

        /* Traitement du cas où une page n'a aucun lien sortant */
        Map<String, PaireGraphePR> copie = new HashMap<String, PaireGraphePR>();
        for (Map.Entry<String, PaireGraphePR> entree : liensDePages.entrySet()) {
            if (entree.getValue().liens.isEmpty()){
                // On ajoute à la page un lien vers toutes les autres pages sauf elle même
                Set<String> toutesLesPages = new HashSet<String>(liensDePages.keySet());
                toutesLesPages.remove(entree.getKey());
                copie.put(entree.getKey(), new PaireGraphePR(1.0/ CG.liensDePages.size(), toutesLesPages));
            } else {
                copie.put(entree.getKey(), new PaireGraphePR(1.0/ CG.liensDePages.size(), entree.getValue().liens));
            }
        }
        liensDePages = copie;

        System.out.println("--------- : ");
        coGr.produire(CG.liensDePages);
    }

}
