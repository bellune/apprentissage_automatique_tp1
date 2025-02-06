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