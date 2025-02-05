import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from itertools import combinations
import re
def calculate_following_followers_ratio(df):
    return df["following"] / (df["followers"] + 1)  # Éviter la division par zéro
#Calcul de la proportion de @, #, http  
def calculate_proportion(feature, symbol, df):
    def count_symbol_excluding_emails(text, symbol):
        words = str(text).split()
        words = [word for word in words if not re.match(r"[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}", word)]  # Exclure les emails
        return sum(1 for word in words if symbol in word) / len(words) if words else 0
    
    return df[feature].apply(lambda x: count_symbol_excluding_emails(x, symbol))

def calculate_tweet_similarity(tweets):
    if len(tweets) < 2:
        return np.nan  # Pas assez de tweets pour comparer
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(tweets)
    similarities = [cosine_similarity(tfidf_matrix[a], tfidf_matrix[b])[0][0] for a, b in combinations(range(len(tweets)), 2)]
    return np.mean(similarities) if similarities else np.nan

def calculate_time_between_tweets(df):
    df["date_tweet"] = pd.to_datetime(df["date_tweet"])
    df.sort_values(by=["nom_utilisateur", "date_tweet"], inplace=True)
    df["temps_entre_tweets"] = df.groupby("nom_utilisateur")["date_tweet"].diff().dt.total_seconds()
    return df.groupby("nom_utilisateur")["temps_entre_tweets"].agg(["mean", "max"])

def calculate_tweets_per_day(df):
    return df["nombre_tweets"] / df["duree_vie_compte"]

