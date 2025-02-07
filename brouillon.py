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
       # proportions_df = fs.calculate_proportion("Tweet", symbols, df_tweets)


        # proportions_df = df_users.apply(
        #     lambda row: fs.calculate_proportion(row['NumberOfTweets'], row['CreatedAt'])
        #     if row['NumberOfTweets'] > 0 else 0, axis=1
        # )

      
        proportions = df_tweets["Tweet"].apply(lambda x: fs.calculate_proportion("Tweet", symbols, df_tweets))
        for symbol in symbols:
            df_tweets[f"proportion_{symbol}"] = proportions.apply(lambda x: x[symbol])

        # print(proportions_df)

        # Fusionner les nouvelles colonnes avec `df_tweets`
       # df_tweets = pd.concat([df_tweets, proportions_df], axis=1)

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
    



def enrich_users_with_tweets(rawpath, raw_tw_path='Pretraitement/RawData/'):
    """ Associe les tweets aux utilisateurs et calcule les métriques. """
    if not os.path.exists(rawpath):
       os.makedirs(rawpath)
     
    chunksize = 100000
    df_tweets = pd.read_csv(rawpath, dtype=str, chunksize=chunksize)

    proportions_grouped = pd.DataFrame()

    try:
        print("Début du traitement des tweets...")

        # Convertir CreatedAt en datetime et filtrer les tweets valides
        df_tweets["CreatedAt"] = pd.to_datetime(df_tweets["CreatedAt"], errors='coerce')
        df_tweets = df_tweets.dropna(subset=["Tweet", "CreatedAt"])

        print(f" Nombre de tweets après nettoyage : {len(df_tweets)}")

        # Calcul des proportions d'URL, mentions et hashtags
        print("Calcul des proportions d'URL, mentions et hashtags...")
        symbols = ["@", "http", "#"]
        proportions = fs.calculate_proportion("Tweet", symbols, df_tweets)

        # Transformer chaque dictionnaire en colonnes individuelles
        for symbol in symbols:
            df_tweets[f"proportion_{symbol}"] = proportions.apply(lambda x: x.get(symbol, 0))

        # Calculer la similarité des tweets par utilisateur
        print("Calcul de la similarité moyenne des tweets...")
        similarity_df = df_tweets.groupby("UserID")["Tweet"].apply(lambda tweets: fs.calculate_tweet_similarity(tweets.tolist())).reset_index()
        similarity_df.rename(columns={"Tweet": "similarite_moyenne_tweets"}, inplace=True)

        # Calculer le temps entre tweets
        print("Calcul des temps entre tweets...")
        time_stats_df = fs.calculate_time_between_tweets(df_tweets).reset_index()
        time_stats_df.rename(columns={"mean": "temps_moyen_entre_tweets", "max": "temps_max_entre_tweets"}, inplace=True)

        # Fusionner les données
        df_tweets = df_tweets.merge(similarity_df, on="UserID", how="left")
        df_tweets = df_tweets.merge(time_stats_df, on="UserID", how="left")

        # Regrouper les moyennes par utilisateur
        cols = [f"proportion_{symbol}" for symbol in symbols] + ["similarite_moyenne_tweets", "temps_moyen_entre_tweets", "temps_max_entre_tweets"]
        proportions_grouped = df_tweets.groupby("UserID")[cols].mean().reset_index()

        print("Toutes les caracteristiiques sont calculées et ajoutées.")

        # Sauvegarde finale
        csv_path = os.path.join(raw_tw_path, "stat_byusers_data.csv")
        csv_folder = os.path.dirname(csv_path)

        if not os.path.exists(csv_folder):
            os.makedirs(csv_folder)

        proportions_grouped.to_csv(csv_path, index=False, encoding='utf-8')

        print("Statistique Tweets terminé avec succès :{csv_path} !")
        return csv_path

    except Exception as e:
        print(f" Erreur lors de l'enrichissement avec les tweets : {e}")
        return proportions_grouped
    


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
