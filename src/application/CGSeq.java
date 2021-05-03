package application;// v0.0 PM, le 18/12/17
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import java.io.IOException;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;


public class CGSeq {
    /* Construction du Graphe : produit un fichier de paires URL <-> PR;liste URL_liens
     * les liens de la liste étant séparés par des espaces
     */
    static Map<String, PaireGraphePR> liensDePages;
    String site;

    public CGSeq() {
        CGSeq.liensDePages = new HashMap<String,PaireGraphePR>();
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

    public static void main (String args[]) {
        // 1 argument : URL du site à évaluer
        CGSeq coGr = null;
        String page;
        String lien;
        URL u ;
        Document doc;
        Set<String> liens = new HashSet<String>();
        LinkedList<String> aTraiter = new LinkedList<String>();

        if (args.length == 1) {
            coGr = new CGSeq();
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

        while (aTraiter.size()>0) {

            // On vide la HashSet liens
            liens.clear();

            // On prend le premier élément de aTraiter et on le retire
            page = aTraiter.pollFirst();

            //System.out.println("page : "+page);
            try {

                // On se connecte au document et on ne garde que les liens
                doc = Jsoup.connect(page).get();
                Elements links = doc.select("a[href]");

                for (Element link : links) {
                    try {
                        // On prend l'url associée à la href
                        u = new URL(link.attr("abs:href"));

                        // élimination des références
                        lien = u.getProtocol()+"://"+u.getAuthority()+u.getFile();

                        try {
                            //les liens ajoutés doivent être valides
                            Jsoup.connect(lien).get();
                            //et sur le site
                            if (! coGr.horsSite(lien)) {
                                // On n'a pas de doublon (set)
                                liens.add(lien);
                            }
                        }
                        catch (IOException iox) {
                            System.out.println("lien inaccessible "+lien+" "+iox);
                        }
                    }
                    catch (MalformedURLException mu) {
                        System.out.println("lien erroné "+link+" "+mu);
                    }
                }
                liens.remove(page); // suppression auto référence éventuelle

                System.out.println("nb liens : "+liens.size());

                // On ajoute dans notre table les liens associés à la page en cours de traitement
                CGSeq.liensDePages.put(page,new PaireGraphePR(1.0, new HashSet<String>()));
                CGSeq.liensDePages.get(page).liens.addAll(liens);

                //ajouter les liens à traiter trouvés dans la page courante
                for (String url : liens) {
                    if (!(CGSeq.liensDePages.containsKey(url) || aTraiter.contains(url))) {
                        aTraiter.add(url);
                    }
                }
            }
            catch (IOException e) { //levée par doc = Jsoup.connect(page).get();
                System.out.println("erreur chargement "+page+" ("+e+")");
            }
        }

        /* Traitement du cas où une page n'a aucun lien sortant */
        Map<String, PaireGraphePR> copie = new HashMap<String, PaireGraphePR>();
        for (Map.Entry<String, PaireGraphePR> entree : liensDePages.entrySet()) {
            if (entree.getValue().liens.isEmpty()){
                // On ajoute à la page un lien vers toutes les autres pages sauf elle même
                Set<String> toutesLesPages = new HashSet<String>(liensDePages.keySet());
                toutesLesPages.remove(entree.getKey());
                copie.put(entree.getKey(), new PaireGraphePR(1.0/ CGSeq.liensDePages.size(), toutesLesPages));
            } else {
                copie.put(entree.getKey(), new PaireGraphePR(1.0/ CGSeq.liensDePages.size(), entree.getValue().liens));
            }
        }
        liensDePages = copie;

        System.out.println("--------- : ");
        coGr.produire(CGSeq.liensDePages);
    }
}
