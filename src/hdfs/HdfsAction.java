package hdfs;

/**
 * Les actions possibles dans HDFS.
 */
public enum HdfsAction {
    /** Reconstition d'un fichier fragmenté. */
    READ,
    /** Sauvegarde d'un fichier fragmenté. */
    WRITE,
    /** Suppression d'un fichier fragmenté. */
    DELETE,
    /** Requête d'un nouveau noeud à initialiser. */
    NEW_NODE,
    /** Requête de vérification d'activité. */
    PING,
    /** Réponse de vérification d'activité. */
    PONG,
    /** Le ping provient d'un noeud inconnu. */
    UNKNOWN_NODE,
    /** On veut connaître la liste des fragments d'un fichier. */
    LIST_FRAGMENTS,
    /** On veut mettre à jour la liste des fichiers. */
    FORCE_RESCAN,
    /** On veut récupérer l'ensemble des noeuds connectés. */
    LIST_NODES
}
