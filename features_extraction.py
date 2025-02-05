import pandas as pd

# Fonction pour extraire des colonnes spécifiques et exporter en Excel
def txt_to_excel_with_selected_columns(txt_file_path, excel_file_path):
    """
    Convertit un fichier texte en un fichier Excel en sélectionnant des colonnes spécifiques.

    :param txt_file_path: Chemin du fichier texte source
    :param excel_file_path: Chemin du fichier Excel de destination
    """
    try:
        # Définir les indices des colonnes à conserver
        # Par exemple, conserver les colonnes 0 (UserID), 2 (LengthOfDescription), 4 (NumberOfFollowings)
        selected_columns_indices = [0, 1, 5, 6, 7, 8, 9]  # À ajuster selon votre besoin
        column_names = [
            'UserID', 'CreatedAt', 'NumberOfFollowings', 'NumberOfFollowers', 
            'NumberOfTweets', 'LengthOfScreenName', 'LengthOfDescriptionInUserProfile',
        ]  # Noms des colonnes à conserver

        # Lire les données ligne par ligne
        with open(txt_file_path, 'r') as file:
            lines = file.readlines()

        # Traiter les lignes
        cleaned_data = []
        problematic_lines = []
        for line in lines:
            # Diviser les colonnes en utilisant des espaces multiples
            columns = line.strip().split()
            
            # Vérifier si la ligne contient toutes les colonnes nécessaires
            if len(columns) > max(selected_columns_indices):
                # Extraire uniquement les colonnes sélectionnées
                selected_columns = [columns[i] for i in selected_columns_indices]
                cleaned_data.append(selected_columns)
            else:
                # Sauvegarder les lignes problématiques
                problematic_lines.append(line.strip())

        # Convertir en DataFrame avec les noms des colonnes sélectionnées
        df = pd.DataFrame(cleaned_data, columns=column_names)

        # Exporter au format Excel
        df.to_excel(excel_file_path, index=False)
        print(f"Les données ont été exportées avec succès vers {excel_file_path}.")

        # Sauvegarder les lignes problématiques (si nécessaire)
        if problematic_lines:
            with open("lignes_problemes.txt", "w") as error_file:
                error_file.write("\n".join(problematic_lines))
            print(f"{len(problematic_lines)} lignes problématiques sauvegardées dans 'lignes_problemes.txt'.")
    except Exception as e:
        print(f"Une erreur s'est produite : {e}")

# Exemple d'utilisation
txt_file_path = "Datasets/content_polluters.txt"  # Fichier texte avec vos données
excel_file_path = "Pretraitement/clean_data_selected.csv"  # Fichier Excel à créer
txt_to_excel_with_selected_columns(txt_file_path, excel_file_path)
