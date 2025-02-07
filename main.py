import sys
import traceback
import functions as fs  # Pour enregistrer les erreurs
import features_extraction as exf  # Module contenant `extraction()`

def main():
    """Point d'entrée principal du programme."""
    try:
        print("Début de l'extraction des données...")
        exf.extraction()   
        print("Extraction terminée avec succès !")

    except Exception as e:
        error_message = f"Une erreur est survenue : {e}\n{traceback.format_exc()}"
        print(error_message)  # Affiche l'erreur complète dans la console
        fs.write_log(error_message)  # Enregistre l'erreur dans un fichier log
        sys.exit(1)  # Quitter le programme avec une erreur

if __name__ == "__main__":
    main()
