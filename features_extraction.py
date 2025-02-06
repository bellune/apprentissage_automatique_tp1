import os
import pandas as pd
import functions as fs
from datetime import datetime


#    Convertit plusieurs fichiers TXT en un seul CSV et ajoute une colonne 'Classe'.
#     Enregistre les lignes problématiques dans un fichier log.
def convert_txts_to_csv(txt_file_paths, classes, output_folder, output_filename, column_names, delimiter="\t"):
   
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
                   
                    if len(df.columns) != len(column_names):
                        fs.write_log(f"Nombre  de colonnes incorrect dans {txt_file_path}. Attendu : {len(column_names)}, Trouvé : {len(df.columns)}")
                        continue
                    
                    df.columns = column_names
                    df["Classe"] = class_label  # Ajout de la colonne 'Classe'
                    all_data.append(df)
                    print(f"Fichier chargé : {txt_file_path} (Classe: {class_label})")
                
                except Exception as e:
                    fs.write_log(f"Erreur lors de la lecture de {txt_file_path} : {e}")
            else:
                fs.write_log(f"Fichier introuvable : {txt_file_path}")

        # Vérifier s'il y a des données avant de fusionner
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df.to_csv(output_csv_path, index=False, encoding="utf-8")
            print(f"Fusion réussie : {output_csv_path}")
        else:
            fs.write_log("Aucun fichier valide n'a été fusionné.")

        return output_csv_path if all_data else None

    except Exception as e:
        print(f"Erreur lors de la fusion des fichiers TXT en CSV : {e}")
        return None


def enrich_users_with_tweetssssss(df_users, df_tweets):
    """ Associe les tweets aux utilisateurs et calcule les métriques """
    try:
            df_tweets["CreatedAt"] = pd.to_datetime(df_tweets["CreatedAt"], errors='coerce')
            df_tweets = df_tweets.dropna(subset=["Tweet", "CreatedAt"])
             
            log_errors = []  # Liste des erreurs
            
            # Associer les tweets par utilisateur
            tweet_counts = df_tweets.groupby("UserID")["Tweet"].count().reset_index()
            tweet_counts.columns = ["UserID", "TotalTweets"]
            df_users = df_users.merge(tweet_counts, on="UserID", how="left")
            df_users["TotalTweets"].fillna(0, inplace=True)

            # Calcul des proportions
            df_users["proportion_url"] = df_users["UserID"].apply(lambda uid: fs.calculate_proportion("Tweet", "http", df_tweets[df_tweets["UserID"] == uid]))
            df_users["proportion_mentions"] = df_users["UserID"].apply(lambda uid: fs.calculate_proportion("Tweet", "@", df_tweets[df_tweets["UserID"] == uid]))
            df_users["proportion_hashtags"] = df_users["UserID"].apply(lambda uid: fs.calculate_proportion("Tweet", "#", df_tweets[df_tweets["UserID"] == uid]))

            df_users["nombre_tweets_repetitifs"] = df_users["UserID"].apply(lambda uid: fs.calculate_tweet_similarity(df_tweets[df_tweets["UserID"] == uid]))

            # Statistiques temporelles
            df_users["temps_moyen_entre_tweets"] = df_users["UserID"].apply(lambda uid: fs.calculate_time_between_tweets(df_tweets[df_tweets["UserID"] == uid]).get("mean", 0))
            df_users["temps_max_entre_tweets"] = df_users["UserID"].apply(lambda uid: fs.calculate_time_between_tweets(df_tweets[df_tweets["UserID"] == uid]).get("max", 0))

          
            return df_users 

    except Exception as e:
        print(f"Erreur lors de l'enrichissement avec les tweets : {e}")
        return df_users



def enrich_users_with_tweets(df_users, df_tweets):
    """ Associe les tweets aux utilisateurs et calcule les métriques de manière optimisée. """
    try:
        print("Début du traitement des tweets...")

        # 1. Convertir CreatedAt en datetime et filtrer les tweets valides
        df_tweets["CreatedAt"] = pd.to_datetime(df_tweets["CreatedAt"], errors='coerce')
        df_tweets = df_tweets.dropna(subset=["Tweet", "CreatedAt"])

        print(f" Nombre de tweets après nettoyage : {len(df_tweets)}")

        # 2. Associer les tweets par utilisateur avec groupby() (beaucoup plus rapide)
       # tweet_counts = df_tweets.groupby("UserID")["Tweet"].count().reset_index()
       # tweet_counts.columns = ["UserID", "TotalTweets"]
       # df_users = df_users.merge(tweet_counts, on="UserID", how="left")
        #df_users["TotalTweets"].fillna(0, inplace=True)

        print("TotalTweets ajouté au dataframe des utilisateurs.")

        # 3. Calcul des proportions avec groupby()
        print("Calcul des proportions d'URL, mentions et hashtags...")

        # def proportion_calculator(df, character):
        #     return df["Tweet"].apply(lambda x: fs.calculate_proportion("Tweet", character, x)).mean()

        # proportions = df_tweets.groupby("UserID").apply(lambda x: pd.Series({
        #     "proportion_url": proportion_calculator(x, "http"),
        #     "proportion_mentions": proportion_calculator(x, "@"),
        #     "proportion_hashtags": proportion_calculator(x, "#"),
        # })).reset_index()

        # df_users = df_users.merge(proportions, on="UserID", how="left")

        print("Proportions calculées et ajoutées.")

        # 4. Calcul des statistiques temporelles avec groupby()
        print(" Calcul des temps moyens et maximum entre tweets...")

        # def time_stats(df):
        #     return pd.Series({
        #         "temps_moyen_entre_tweets": fs.calculate_time_between_tweets(df)["mean"],
        #         "temps_max_entre_tweets": fs.calculate_time_between_tweets(df)["max"],
        #     })

        # time_stats_df = df_tweets.groupby("UserID").apply(time_stats).reset_index()
        # df_users = df_users.merge(time_stats_df, on="UserID", how="left")


             # Calcul des statistiques temporelles en évitant l'erreur de type
        # def time_stats(df):
        #     result = fs.calculate_time_between_tweets(df)
        #     if isinstance(result, dict):
        #         return pd.Series({
        #             "temps_moyen_entre_tweets": result.get("mean", 0),
        #             "temps_max_entre_tweets": result.get("max", 0),
        #         })
        #     else:
        #         return pd.Series({
        #             "temps_moyen_entre_tweets": 0,
        #             "temps_max_entre_tweets": 0,
        #         })

        # time_stats_df = df_tweets.groupby("UserID").apply(time_stats).reset_index()
        # df_users = df_users.merge(time_stats_df, on="UserID", how="left")
        print("Calcul des temps moyens et maximum entre tweets...")
       
        symbols = ["@", "http", "#"]
        print(df_tweets)
        # Appliquer la fonction sur `df_tweets`
        proportions_df = fs.calculate_proportion("Tweet", symbols, df_tweets)


        # proportions_df = df_users.apply(
        #     lambda row: fs.calculate_proportion(row['NumberOfTweets'], row['CreatedAt'])
        #     if row['NumberOfTweets'] > 0 else 0, axis=1
        # )

      
        # proportions = df_tweets["tweets"].apply(lambda x: fs.calculate_proportion("tweets", symbols, df_tweets))
        # for symbol in symbols:
        #     df_users[f"proportion_{symbol}"] = proportions.apply(lambda x: x[symbol])

        # print(proportions_df)

        # Fusionner les nouvelles colonnes avec `df_tweets`
        df_tweets = pd.concat([df_tweets, proportions_df], axis=1)

        # Regrouper les moyennes par utilisateur
        proportions_grouped = df_tweets.groupby("UserID")[symbols].mean().reset_index()

        # Fusionner avec `df_users`
        df_users = df_users.merge(proportions_grouped, on="UserID", how="left")

        print("Statistiques temporelles ajoutées.")

        print("Enrichissement des utilisateurs terminé avec succès !")
        return df_users

    except Exception as e:
        print(f"Erreur lors de l'enrichissement avec les tweets : {e}")
        return df_users




def process_users(csv_file_path, tweets_csv_path, processed_folder):
    """ Charge les utilisateurs, enrichit avec les tweets et sauvegarde. """
    try:
        if not os.path.exists(processed_folder):
            os.makedirs(processed_folder)

        df_users = pd.read_csv(csv_file_path, dtype=str)
        df_tweets = pd.read_csv(tweets_csv_path, dtype=str)

        log_errors = []  # Liste des erreurs

        # Sélection des colonnes utiles
    
        selected_columns = [
            'UserID', 'CreatedAt', 'NumberOfFollowings', 'NumberOfFollowers', 
            'NumberOfTweets', 'LengthOfScreenName', 'LengthOfDescriptionInUserProfile', 'Classe'
        ]

        missing_cols = [col for col in selected_columns if col not in df_users.columns]

        if missing_cols:
            fs.write_log(f"Colonnes manquantes dans df_users : {missing_cols}")

        

        # Conversion des colonnes numériques
        numeric_cols = ['NumberOfFollowings', 'NumberOfFollowers', 'NumberOfTweets']
        try:
            df_users[numeric_cols] = df_users[numeric_cols].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
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
        df_users = enrich_users_with_tweets(df_users, df_tweets)

        # Sauvegarde finale
        processed_csv_path = os.path.join(processed_folder, "final_users_data.csv")
        df_users.to_csv(processed_csv_path, index=False, encoding='utf-8')


        print(f"Données finales enregistrées dans {processed_csv_path}")
        return processed_csv_path

    except Exception as e:
        print(f"Erreur dans le traitement des utilisateurs : {e}")
        return None


# user_columns = [
#     "UserID", "CreatedAt", "CollectedAt", "NumberOfFollowings", "NumberOfFollowers",
#     "NumberOfTweets", "LengthOfScreenName", "LengthOfDescriptionInUserProfile"
# ]

# # Colonnes attendues pour les tweets
# tweet_columns = ["UserID", "TweetID", "Tweet", "CreatedAt"]


# # Fichiers utilisateurs et classes associées
# user_txt_files = ["Datasets/content_polluters.txt", "Datasets/legitimate_users.txt"]
# user_classes = ["1", "0"]
# user_csv_path = convert_txts_to_csv(user_txt_files, user_classes, "Pretraitement/RawData", "users.csv", user_columns)

# # Fichiers tweets et classes associées
# tweets_txt_files = ["Datasets/content_polluters_tweets.txt", "Datasets/legitimate_users_tweets.txt"]
# tweets_classes = ["1", "0"]
# tweets_csv_path = convert_txts_to_csv(tweets_txt_files, tweets_classes, "Pretraitement/RawData", "tweets.csv", tweet_columns)



# Traitement des utilisateurs avec enrichissement des tweets
# if user_csv_path and tweets_csv_path:
user_csv_path = "Pretraitement/RawData/users.csv"
tweets_csv_path = "Pretraitement/RawData/tweets.csv"
final_data_path = process_users(user_csv_path, tweets_csv_path, "Pretraitement/FinalData")
