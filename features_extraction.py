import os
import pandas as pd
import functions as fs


def convert_txts_to_csv(txt_file_paths, classes, output_folder, output_filename, log_file="log_erreurs.txt", delimiter="\t"):
    """
    Convertit plusieurs fichiers TXT en un seul CSV et ajoute une colonne 'Classe'.
    Enregistre les lignes problématiques dans un fichier log.
    """
    try:
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        output_csv_path = os.path.join(output_folder, output_filename)

        all_data = []
        log_errors = []  # Liste des erreurs

        for txt_file_path, class_label in zip(txt_file_paths, classes):
            if os.path.exists(txt_file_path):
                try:
                    df = pd.read_csv(txt_file_path, sep=delimiter, header=None, on_bad_lines="skip", dtype=str)
                    df["Classe"] = class_label  # Ajout de la colonne 'Classe'
                    all_data.append(df)
                    print(f"Fichier chargé : {txt_file_path} (Classe: {class_label})")
                except Exception as e:
                    log_errors.append(f"Erreur lors de la lecture de {txt_file_path} : {e}")
            else:
                log_errors.append(f"⚠️ Fichier introuvable : {txt_file_path}")

        # Vérifier s'il y a des données avant de fusionner
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df.to_csv(output_csv_path, index=False, encoding="utf-8")
            print(f"Fusion réussie : {output_csv_path}")
        else:
            log_errors.append("Aucun fichier valide n'a été fusionné.")

        # Sauvegarde des erreurs dans un fichier log
        if log_errors:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write("\n".join(log_errors) + "\n")
            print(f"⚠️ Erreurs enregistrées dans {log_file}")

        return output_csv_path if all_data else None

    except Exception as e:
        print(f"Erreur lors de la fusion des fichiers TXT en CSV : {e}")
        return None


def enrich_users_with_tweets(df_users, df_tweets, log_file="log_erreurs.txt"):
    """ Associe les tweets aux utilisateurs et calcule les métriques """
    try:
        df_tweets["Timestamp"] = pd.to_datetime(df_tweets["Timestamp"], errors='coerce')
        df_tweets = df_tweets.dropna(subset=["Tweet", "Timestamp"])

        log_errors = []  # Liste des erreurs

        # Associer les tweets par utilisateur
        tweet_counts = df_tweets.groupby("UserID")["Tweet"].count().reset_index()
        tweet_counts.columns = ["UserID", "TotalTweets"]
        df_users = df_users.merge(tweet_counts, on="UserID", how="left")
        df_users["TotalTweets"].fillna(0, inplace=True)

        # Identifie les utilisateurs sans tweets
        missing_tweets = df_users[df_users["TotalTweets"] == 0]["UserID"].tolist()
        if missing_tweets:
            log_errors.append(f"⚠️ {len(missing_tweets)} utilisateurs n'ont aucun tweet.")

        # Sauvegarde des erreurs dans un fichier log
        if log_errors:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write("\n".join(log_errors) + "\n")
            print(f"⚠️ Erreurs enregistrées dans {log_file}")

        return df_users

    except Exception as e:
        print(f"Erreur lors de l'enrichissement avec les tweets : {e}")
        return df_users



def process_users(csv_file_path, tweets_csv_path, processed_folder, log_file="log_erreurs.txt"):
    """ Charge les utilisateurs, enrichit avec les tweets et sauvegarde. """
    try:
        if not os.path.exists(processed_folder):
            os.makedirs(processed_folder)

        df_users = pd.read_csv(csv_file_path, dtype=str)
        df_tweets = pd.read_csv(tweets_csv_path, sep="\t", dtype=str, on_bad_lines="skip")

        log_errors = []  # Liste des erreurs

        # Sélection des colonnes utiles
        selected_columns_indices = [0, 1, 3, 4, 5, 6, 7]
        column_names = [
            'UserID', 'CreatedAt', 'NumberOfFollowings', 'NumberOfFollowers', 
            'NumberOfTweets', 'LengthOfScreenName', 'LengthOfDescriptionInUserProfile'
        ]

        try:
            df_users = df_users.iloc[:, selected_columns_indices]
            df_users.columns = column_names
        except Exception as e:
            log_errors.append(f"Problème lors de la sélection des colonnes : {e}")

        # 🔹 Conversion des colonnes numériques
        numeric_cols = ['NumberOfFollowings', 'NumberOfFollowers', 'NumberOfTweets']
        try:
            df_users[numeric_cols] = df_users[numeric_cols].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
        except Exception as e:
            log_errors.append(f"Erreur lors de la conversion numérique : {e}")

        # 🔹 Conversion de CreatedAt en datetime
        try:
            df_users["CreatedAt"] = pd.to_datetime(df_users["CreatedAt"], errors='coerce')
        except Exception as e:
            log_errors.append(f"Erreur lors de la conversion de CreatedAt : {e}")

            # 🔹 Conversion de CreatedAt en datetime
        try:
            df_users["CreatedAt"] = pd.to_datetime(df_users["CreatedAt"], errors='coerce')
        except Exception as e:
            log_errors.append(f"⚠️ Erreur lors de la conversion de CreatedAt : {e}")

        # 🔹 Ajouter la colonne "DaysSinceCreation"
        df_users["DaysSinceCreation"] = df_users["CreatedAt"].apply(fs.calculer_duree_compte)

        # Ajouter la colonne du ratio
        df_users['Following/Followers Ratio'] = df_users.apply(
            lambda row: fs.calculate_following_followers_ratio(row['NumberOfFollowings'], row['NumberOfFollowers'])
            if row['NumberOfFollowers'] > 0 else 0, axis=1
        )

        df_users['tweets_par_jour'] = df_users.apply(
            lambda row: fs.calculer_tweets_par_jour(row['NumberOfTweets'], row['CreatedAt'])
            if row['NumberOfTweets'] > 0 else 0, axis=1
        )

        # Appel de l'enrichissement des tweets
        df_users = enrich_users_with_tweets(df_users, df_tweets, log_file)

        # Sauvegarde finale
        processed_csv_path = os.path.join(processed_folder, "final_users_data.csv")
        df_users.to_csv(processed_csv_path, index=False, encoding='utf-8')

        # Sauvegarde des erreurs dans un fichier log
        if log_errors:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write("\n".join(log_errors) + "\n")
            print(f"⚠️ Erreurs enregistrées dans {log_file}")

        print(f"Données finales enregistrées dans {processed_csv_path}")
        return processed_csv_path

    except Exception as e:
        print(f"Erreur dans le traitement des utilisateurs : {e}")
        return None




# Fichiers utilisateurs et classes associées
user_txt_files = ["Datasets/content_polluters.txt", "Datasets/legitimate_users.txt"]
user_classes = ["1", "0"]
user_csv_path = convert_txts_to_csv(user_txt_files, user_classes, "Pretraitement/RawData", "users.csv")

# Fichiers tweets et classes associées
tweets_txt_files = ["Datasets/content_polluters_tweets.txt", "Datasets/legitimate_users_tweets.txt"]
tweets_classes = ["1", "0"]
tweets_csv_path = convert_txts_to_csv(tweets_txt_files, tweets_classes, "Pretraitement/RawData", "tweets.csv")


# Traitement des utilisateurs avec enrichissement des tweets
if user_csv_path and tweets_csv_path:
    final_data_path = process_users(user_csv_path, tweets_csv_path, "Pretraitement/FinalData")
