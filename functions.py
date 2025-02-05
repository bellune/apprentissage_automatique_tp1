import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from itertools import combinations
import re
def calculate_following_followers_ratio(following, followers ):
    return following / (followers + 1)  # Éviter la division par zéro
#Calcul de la proportion de @, #, http  
def calculate_proportion(feature, symbol, df):
    def count_symbol_excluding_emails(text, symbol):
        words = str(text).split()
        words = [word for word in words if not re.match(r"[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}", word)]  # Exclure les emails
        return sum(1 for word in words if symbol in word) / len(words) if words else 0
    
    return feature.apply(lambda x: count_symbol_excluding_emails(x, symbol))

def calculate_tweet_similarity(tweets):
    if len(tweets) < 2:
        return np.nan  # Pas assez de tweets pour comparer
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(tweets)
    similarities = [cosine_similarity(tfidf_matrix[a], tfidf_matrix[b])[0][0] for a, b in combinations(range(len(tweets)), 2)]
    return np.mean(similarities) if similarities else np.nan

def calculate_time_between_tweets(df):
    df["date_tweet"] = pd.to_datetime(df["date_tweet"])
    df.sort_values(by=["userID", "date_tweet"], inplace=True)
    df["temps_entre_tweets"] = df.groupby("nom_utilisateur")["date_tweet"].diff().dt.total_seconds()
    return df.groupby("nom_utilisateur")["temps_entre_tweets"].agg(["mean", "max"])

#Calcule le nombre moyen de tweets par jour."""
def calculer_tweets_par_jour(nombre_tweets, duree_vie_compte):
    return nombre_tweets / duree_vie_compte if duree_vie_compte > 0 else 0


# Charger les fichiers
df_pollueurs = pd.read_csv("pollueurs.txt", delimiter="\t")
df_legitimes = pd.read_csv("legitimes.txt", delimiter="\t")
df_pollueurs["classe"] = 1
df_legitimes["classe"] = 0
df = pd.concat([df_pollueurs, df_legitimes], ignore_index=True)
# Calcul des caractéristiques
df["rapport_following_followers"] = calculate_following_followers_ratio(df)
df["proportion_url"] = calculate_proportion("tweets", "http", df)
df["proportion_mentions"] = calculate_proportion("tweets", "@", df)
df["proportion_hashtags"] = calculate_proportion("tweets", "#", df)
df["tweets_par_jour"] = calculate_tweets_per_day(df)
df["similarite_moyenne_tweets"] = df.groupby("nom_utilisateur")["tweets"].transform(calculate_tweet_similarity)
df["nombre_tweets_repetitifs"] = calculate_repetitive_tweets(df)
time_stats = calculate_time_between_tweets(df)
df["temps_moyen_entre_tweets"] = time_stats["mean"]
df["temps_max_entre_tweets"] = time_stats["max"]
