import os
import pandas as pd
import functions as fs
from sklearn.model_selection import train_test_split
import csv  # Pour gérer les erreurs de formatage
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
import numpy as np






def convert_txts_to_csv(txt_file_paths, classes, output_folder, output_filename, column_names, delimiter="\t"):
    
    #Convertit plusieurs fichiers TXT en un seul CSV et ajoute une colonne 'Classe'.
    
    try:
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        output_csv_path = os.path.join(output_folder, output_filename)
        all_data = []
        log_errors = []  # Liste des erreurs
        missing_lines_report = {}

        for txt_file_path, class_label in zip(txt_file_paths, classes):
            if os.path.exists(txt_file_path):
                try:
                    # Étape 1 : Compter le nombre de lignes dans le fichier TXT
                    total_lines_txt = fs.count_lines_in_txt(txt_file_path)

                    # Étape 2 : Charger les données / on_bad_lines="skip"
                    df = pd.read_csv(txt_file_path, sep=delimiter, header=None,quoting=csv.QUOTE_NONE)

                    # Étape 3 : Vérifier si toutes les lignes ont été chargées
                    total_lines_loaded = len(df)
                    if total_lines_loaded < total_lines_txt:
                        missing_lines = total_lines_txt - total_lines_loaded
                        missing_lines_report[txt_file_path] = missing_lines
                        fs.write_log(f"{missing_lines} lignes manquantes dans {txt_file_path} ({total_lines_loaded}/{total_lines_txt} lignes importées).")

                    # Vérification des colonnes
                    if len(df.columns) != len(column_names):
                        fs.write_log(f"Nombre de colonnes incorrect dans {txt_file_path}. Attendu : {len(column_names)}, Trouvé : {len(df.columns)}")
                        continue
                    
                    df.columns = column_names
                    df["Classe"] = class_label  # Ajout de la colonne 'Classe'
                    all_data.append(df)
                    print(f"Fichier chargé : {txt_file_path} (Classe: {class_label}) - {total_lines_loaded}/{total_lines_txt} lignes importées.")

                except Exception as e:
                    fs.write_log(f"Erreur lors de la lecture de {txt_file_path} : {e}")
            else:
                fs.write_log(f"Fichier introuvable : {txt_file_path}")

        # Étape 5 : Fusionner tous les fichiers
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)

           

            #Eecrire dans le fichier csv
            final_df.to_csv(output_csv_path, index=False, encoding="utf-8")
            print(f"Fusion réussie : {output_csv_path}")

            # Étape 6 : Vérification finale et rapport
            if missing_lines_report:
                fs.write_log(" Rapport des lignes manquantes par fichier :")
                for file, missing in missing_lines_report.items():
                   fs.write_log(f"- {file} : {missing} lignes non chargées.")

            return output_csv_path
        else:
            fs.write_log(" Aucun fichier valide n'a été fusionné.")
            return None

    except Exception as e:
        fs.write_log(f" Erreur lors de la fusion des fichiers TXT en CSV : {e}")
        return None




    


def users_with_tweets(rawpath, raw_tw_path='Pretraitement/RawData'):
    
    #Associe les tweets aux utilisateurs et calcule les métriques. 
   # Vérifie que toutes les lignes du fichier CSV ont été chargées.
    
    if not os.path.exists(rawpath) :
        os.makedirs(rawpath)

    if not os.path.exists(raw_tw_path) :
        os.makedirs(raw_tw_path)

    try:
        print("Début du traitement des tweets...")

        #Etape 1 : Compter les lignes du fichier avant chargement
        total_lines_csv = fs.count_lines_in_csv(rawpath)

        # Étape 2 : Charger le fichier CSV
        df_tweets = pd.read_csv(rawpath, dtype=str)
        
        # Vérification si toutes les lignes ont été chargées
        total_lines_loaded = len(df_tweets)
        if total_lines_loaded < total_lines_csv:
            missing_lines = total_lines_csv - total_lines_loaded
            fs.write_log(f" {missing_lines} lignes manquantes dans {rawpath} ({total_lines_loaded}/{total_lines_csv} lignes importées).")

        print(f" Nombre de tweets après chargement : {len(df_tweets)}")

         #pretraitement des donnnees 
          # 1. Suppression des doublons
        df_tweets.drop_duplicates()
        print(f"Doublons supprimés. Nombre de lignes après suppression : {df_tweets.shape[0]}")

        # Étape 3 : Convertir CreatedAt en datetime 
        df_tweets["CreatedAt"] = pd.to_datetime(df_tweets["CreatedAt"], errors='coerce')
        df_tweets = df_tweets.dropna(subset=["Tweet", "CreatedAt"])

        print(f" Nombre de tweets après nettoyage : {len(df_tweets)}")

        # Étape 4 : Calcul des proportions d'URL, mentions et hashtags
        print("Calcul des proportions d'URL, mentions et hashtags...")
        symbols = ["@", "http", "#"]
        proportions = fs.calculate_proportion("Tweet", symbols, df_tweets)

        # Transformer chaque dictionnaire en colonnes individuelles
        for symbol in symbols:
            df_tweets[f"proportion_{symbol}"] = proportions.apply(lambda x: x.get(symbol, 0))

        # Étape 5 : Calcul de la répétition moyenne des tweets
        print("Calcul de la repetition moyenne des tweets...")
        df_avg_repetition = fs.average_tweet_repetition(df_tweets)

        # Fusionner avec df_tweets
        df_tweets = df_tweets.merge(df_avg_repetition, on="UserID", how="left")
        # df_tweets["repetition_moyenne_tweets"].fillna(df_tweets.median(), inplace=True)  # Remplacer NaN par la mediane
        #pretraitement des donnnees 
       


        # Étape 6 : Calcul du temps entre tweets
        print("Calcul des temps entre tweets...")
        time_stats_df = fs.calculate_time_between_tweets(df_tweets).reset_index()
        time_stats_df.rename(columns={"mean": "temps_moyen_entre_tweets", "max": "temps_max_entre_tweets"},inplace=True)
        # print(time_stats_df)
        # df_tweets["repetition_moyenne_tweets"].fillna(0)

        # Fusionner les données
        df_tweets = df_tweets.merge(time_stats_df, on="UserID", how="left")

        # Étape 7 : Regrouper les moyennes par utilisateur
        cols = [f"proportion_{symbol}" for symbol in symbols] + ["repetition_moyenne_tweets", "temps_moyen_entre_tweets", "temps_max_entre_tweets"]
        proportions_grouped = df_tweets.groupby("UserID")[cols].mean().reset_index()

       

        print("Toutes les métriques calculées et ajoutées.")

        # Étape 8 : Sauvegarde finale
        csv_path = os.path.join(raw_tw_path, "stat_byusers_data.csv")
        csv_folder = os.path.dirname(csv_path)

        if not os.path.exists(csv_folder):
            os.makedirs(csv_folder)

        # Vérifier que proportions_grouped n'est pas vide avant d'écrire le fichier
        if proportions_grouped is None or proportions_grouped.empty:
            print("Erreur : Aucune donnée disponible pour sauvegarde !")
            return None

        # Sauvegarde du fichier CSV
        proportions_grouped.to_csv(csv_path, index=False, encoding='utf-8')

        # Vérification que le fichier a bien été créé
        if not os.path.exists(csv_path):
            print(f"Erreur : Le fichier {csv_path} n'a pas été généré !")
            return None

        print(f"Tweets traités avec succès : {csv_path}!")
        return csv_path

    except Exception as e:
        fs.write_log(f"Erreur lors du traitement des tweets : {e}")
        return None






def process_users(csv_file_path, tweets_csv_path, processed_folder):
    """ Charge les utilisateurs, enrichit avec les tweets et sauvegarde. """
    try:
        if not os.path.exists(processed_folder):
            os.makedirs(processed_folder)

        df_users = pd.read_csv(csv_file_path)
        df_tweets = pd.read_csv(tweets_csv_path)

        log_errors = []  # Liste des erreurs

        # Sélection des colonnes utiles
    
        selected_columns = [
            'UserID', 'CreatedAt', 'CollectedAt', 'NumberOfFollowings', 'NumberOfFollowers', 
            'NumberOfTweets', 'LengthOfScreenName', 'LengthOfDescriptionInUserProfile', 'Classe'
        ]

        missing_cols = [col for col in selected_columns if col not in df_users.columns]

        if missing_cols:
            fs.write_log(f"Colonnes manquantes dans df_users : {missing_cols}")

         #pretraitement des donnnees 
        df_users = pretraitement_data(df_users)

        # Conversion des colonnes numériques
        numeric_cols = ['NumberOfFollowings', 'NumberOfFollowers', 'NumberOfTweets']
        try:
            df_users[numeric_cols] = df_users[numeric_cols].apply(pd.to_numeric, errors='coerce').astype(int)
        except Exception as e:
            fs.write_log(f"Erreur lors de la conversion numérique : {e}")

        # Conversion de CreatedAt en datetime
        try:
            df_users["CreatedAt"] = pd.to_datetime(df_users["CreatedAt"], errors='coerce')
        except Exception as e:
            fs.write_log(f"Erreur lors de la conversion de CreatedAt : {e}")

            # Conversion de CreatedAt en datetime
        try:
            df_users["CreatedAt"] = pd.to_datetime(df_users["CreatedAt"], errors='coerce')
        except Exception as e:
            fs.write_log(f"Erreur lors de la conversion de CreatedAt : {e}")

        # Ajouter la colonne "DaysSinceCreation"
        # df_users["DaysSinceCreation"] = df_users["CreatedAt"].apply(fs.calculer_duree_compte)

        df_users['DaysSinceCreation'] = df_users.apply(
            lambda row: fs.calculer_duree_compte(row['CreatedAt'], row['CollectedAt'])
            if row['CreatedAt'] and row['CollectedAt']  else 0, axis=1
        )

        # Ajouter la colonne du ratio
        df_users['Following/Followers Ratio'] = df_users.apply(
            lambda row: fs.calculate_following_followers_ratio(row['NumberOfFollowings'], row['NumberOfFollowers'])
            if row['NumberOfFollowers'] > 0 else 0, axis=1
        )

        df_users['tweets_by_day'] = df_users.apply(
            lambda row: fs.calculer_tweets_par_jour(row['NumberOfTweets'], row['DaysSinceCreation'])
            if row['NumberOfTweets'] > 0 else 0, axis=1
        )

      
        df_final = df_users.merge(df_tweets, on="UserID", how="left")

        df_final = pretraitement_data(df_final)


        df_final = df_final[['LengthOfScreenName', 'LengthOfDescriptionInUserProfile', 'DaysSinceCreation', 
                             'NumberOfFollowings', 'NumberOfFollowers', 'Following/Followers Ratio', 'tweets_by_day',
                             'proportion_http', 'proportion_@','proportion_#','temps_moyen_entre_tweets', 'temps_max_entre_tweets','repetition_moyenne_tweets', 'Classe']]

        # Sauvegarde finale
        processed_csv_path = os.path.join(processed_folder, "final_users_data.csv")
        df_final.to_csv(processed_csv_path, index=False, encoding='utf-8')


        print(f"Données finales enregistrées dans {processed_csv_path}")
        return processed_csv_path

    except Exception as e:
        print(f"Erreur dans le traitement des utilisateurs : {e}")
        return None





def pretraitement_data(df):
   
    try:

        # 1. Suppression des doublons
        df.drop_duplicates()
        print(f"Doublons supprimés. Nombre de lignes après suppression : {df.shape[0]}")


        classe_col = df["Classe"]  # Sauvegarde la colonne "Classe"
        # df = df.drop_duplicates(subset=df.columns.drop("Classe"))  # Supprime les doublons sans toucher "Classe

        # 2. Remplacement des valeurs manquantes par la médiane avec Scikit-learn
       # Sélectionner uniquement les colonnes numériques

        num_cols = df.select_dtypes(include=["int64", "float64"]).columns
        # print(num_cols)
        df[num_cols] = df[num_cols].replace({" ": "", "NAN": np.nan}).apply(pd.to_numeric, errors="coerce")
        # Création du transformateur d'imputation
        imputer = SimpleImputer(strategy="median")

        # Appliquer l'imputation uniquement sur les colonnes numériques
        df[num_cols] = imputer.fit_transform(df[num_cols])

    
        df["Classe"] = classe_col  # Restaure la colonne "Classe"

        return df

    except Exception as e:
        print(f"Erreur lors du pretraitement des données : {e}")
        return None
    




def prepare_and_split_data(file_path, output_folder="Pretraitement/train", output_folder_test = "Pretraitement/test", test_size=0.2, random_state=42):
    """
    Prépare et sépare les données en ensembles d'entraînement (80%) et de test (20%).

    - Supprime les doublons.
    - Remplace les valeurs manquantes par la médiane.
    - Normalise les données (Z-score).
    - Sépare les données en conservant un équilibre entre les classes.
    """
    try:
        if not os.path.exists(output_folder) & os.path.exists(output_folder_test):
            os.makedirs(output_folder)
            os.makedirs(output_folder_test)

        # 1. Charger les données
        df = pd.read_csv(file_path)
        print(f" Données chargées : {df.shape[0]} lignes, {df.shape[1]} colonnes.")

        # Vérifier si la colonne de classe existe
        if "Classe" not in df.columns:
            raise ValueError("Erreur : La colonne 'Classe' est absente du fichier CSV.")
        
        # 2. Suppression des doublons et remplacement des valeurs manquantes par la mediane
        # df = pretraitement_data(df)

      
        #  3. Normalisation des données (Z-score)
        classe_col = df["Classe"]  # Sauvegarde la colonne "Classe"
        scaler = StandardScaler()
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
        df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
        print("Normalisation des données terminée (Z-score).")

        # 4. Restaure la colonne "Classe"
        df["Classe"] = classe_col  


         # 5. Un sous-ensemble où les pollueurs représentent 5 % des utilisateurs légitimes.
        df_dq = get_unbalance_data(df)

        # 6. Séparation en train (80%) et test (20%) avec stratification sur "Classe"
        df_train, df_test = train_test_split(df, test_size=test_size, stratify=df["Classe"], random_state=random_state)
        print(f"Données séparées : {df_train.shape[0]} pour l'entraînement, {df_test.shape[0]} pour le test.")

        # 7. Séparation en train (80%) et test (20%) avec stratification sur "Classe" des donnees desequilibree
        df_train_unb, df_test_unb = train_test_split(df_dq, test_size=test_size, stratify=df_dq["Classe"], random_state=random_state)
        print(f"Données désequilibrées séparées : {df_train_unb.shape[0]} pour l'entraînement, {df_test_unb.shape[0]} pour le test.")

        # 8. Sauvegarde des fichiers
        train_file = os.path.join(output_folder, "training_data.csv")
        test_file = os.path.join(output_folder_test, "test_data.csv")
        train_file_unb = os.path.join(output_folder, "training_unbalance_data.csv")
        test_file_unb = os.path.join(output_folder_test, "test_unbalance_data.csv")

        df_train.to_csv(train_file, index=False, encoding="utf-8")
        df_test.to_csv(test_file, index=False, encoding="utf-8")
        df_train_unb.to_csv(train_file_unb, index=False, encoding="utf-8")
        df_test_unb.to_csv(test_file_unb, index=False, encoding="utf-8")

        print(f"Fichiers enregistrés :\n - {train_file} /  {train_file_unb}  \n - {test_file} / {test_file_unb}")

        return {"train": [train_file, train_file_unb], "test": [test_file, test_file_unb]}

    except Exception as e:
        print(f"Erreur lors du traitement des données : {e}")
        return None




def get_unbalance_data(df):
    # Séparer les légitimes et les pollueurs
    df_legitimes = df[df["Classe"] == 0]  # Utilisateurs légitimes
    df_pollueurs = df[df["Classe"] == 1]  # Pollueurs

    # Calculer le nombre de pollueurs à garder (5 % des légitimes)
    nb_legitimes = len(df_legitimes)
    nb_pollueurs_a_garder = int(0.05 * nb_legitimes)  # 5 % des légitimes
    print(nb_pollueurs_a_garder)

    # Sélectionner un échantillon de pollueurs (aléatoire)
    df_pollueurs_reduits = df_pollueurs.sample(n=nb_pollueurs_a_garder, random_state=42)

    # Construire le nouveau sous-ensemble
    df_sous_ensemble = pd.concat([df_legitimes, df_pollueurs_reduits], ignore_index=True)

    # Vérifier la distribution des classes
    print(f"Nombre de legitimes : {nb_legitimes} Nombre de pollueurs(5%) : {nb_pollueurs_a_garder}. total : {df_sous_ensemble["Classe"].value_counts()}")
    return df_sous_ensemble





def extraction():
    user_columns = [
        "UserID", "CreatedAt", "CollectedAt", "NumberOfFollowings", "NumberOfFollowers",
        "NumberOfTweets", "LengthOfScreenName", "LengthOfDescriptionInUserProfile"
    ]

    # Colonnes attendues pour les tweets
    tweet_columns = ["UserID", "TweetID", "Tweet", "CreatedAt"]


    # Fichiers utilisateurs et classes associées
    user_txt_files = ["Datasets/content_polluters.txt", "Datasets/legitimate_users.txt"]
    user_classes = ["1", "0"]
    user_csv_path = convert_txts_to_csv(user_txt_files, user_classes, "Pretraitement/RawData", "users.csv", user_columns)

    # Fichiers tweets et classes associées
    tweets_txt_files = ["Datasets/content_polluters_tweets.txt", "Datasets/legitimate_users_tweets.txt"]
    tweets_classes = ["1", "0"]
    tweets_csv_path = convert_txts_to_csv(tweets_txt_files, tweets_classes, "Pretraitement/RawData", "tweets.csv", tweet_columns)

    # Traitement des utilisateurs avec des tweets
    # if user_csv_path and tweets_csv_path:
    # user_csv_path = "Pretraitement/RawData/users.csv"
    # tweets_csv_path = "Pretraitement/RawData/tweets.csv"

    # user_csv_path = "Pretraitement/RawData/users.csv"
    # tweets_csv_path = "Pretraitement/RawData/tweets.csv"
    
    if user_csv_path and tweets_csv_path:
        tweets_byuser_path = users_with_tweets(tweets_csv_path)
        if user_csv_path and tweets_csv_path:
                final_data_path = process_users(user_csv_path, tweets_byuser_path, "Pretraitement/FinalData")
                        # Exemple d'utilisation
                if final_data_path:
                        final_data_path = "Pretraitement/FinalData/final_users_data.csv"  # Remplace par le chemin de ton fichier
                        data_files = prepare_and_split_data(final_data_path)


   







