import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from itertools import combinations
import re
import faiss



def calculate_following_followers_ratio(following, followers ):
    return round(following / (followers + 1), 3)  # Éviter la division par zéro


#Calcul de la proportion de @, #, http  
def calculate_proportion(feature, symbols, df):
    def count_symbols_excluding_emails(text, symbols):
        words = str(text).split()
        words = [word for word in words if not re.match(r"[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}", word)]  # Exclure les emails
        return {symbol: sum(1 for word in words if symbol in word) / len(words) if words else 0  for symbol in symbols }
    
    return df[feature].apply(lambda x: count_symbols_excluding_emails(x, symbols))


def calculate_tweet_similarity(tweets):
    if len(tweets) < 2:
        return np.nan  # Pas assez de tweets pour comparer
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(tweets)
    similarities = [cosine_similarity(tfidf_matrix[a], tfidf_matrix[b])[0][0] for a, b in combinations(range(len(tweets)), 2)]
    return np.mean(similarities) if similarities else np.nan



def calculate_tweet_similarity(tweets):
    if len(tweets) < 2:
        return np.nan

    vectorizer = TfidfVectorizer(max_features=5000)
    tfidf_matrix = vectorizer.fit_transform(tweets).toarray()

    index = faiss.IndexFlatL2(tfidf_matrix.shape[1])
    index.add(tfidf_matrix)

    distances, _ = index.search(tfidf_matrix, k=2)
    similarities = 1 - distances[:, 1]  
    
    return np.mean(similarities)



def remove_urls(text):
    #Supprime les URL d'un texte donné.
    return re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE).strip()

def average_tweet_repetition(df):
    
    #Calcule la répétition moyenne des tweets pour chaque utilisateur après suppression des URL.

    if "UserID" not in df.columns or "Tweet" not in df.columns:
        raise ValueError("Le DataFrame doit contenir les colonnes 'UserID' et 'Tweet'.")

    # 1. Supprimer les URL des tweets
    df["Tweet_clean"] = df["Tweet"].apply(remove_urls)

    # 2. Compter combien de fois chaque tweet apparaît après nettoyage
    tweet_counts = df.groupby(["UserID", "Tweet_clean"]).size().reset_index(name="nb_occurrences")

    # 3. Nombre total de tweets par utilisateur
    total_tweets_per_user = df.groupby("UserID")["Tweet"].count().reset_index(name="nb_total_tweets")

    # 4. Nombre total de tweets répétés par utilisateur (somme des tweets qui apparaissent plus d'une fois)
    repeated_tweets = tweet_counts[tweet_counts["nb_occurrences"] > 1]  # Filtrer les tweets répétés
    repeated_tweet_counts = repeated_tweets.groupby("UserID")["nb_occurrences"].sum().reset_index()

    # 5. Fusionner les données
    df_final = total_tweets_per_user.merge(repeated_tweet_counts, on="UserID", how="left").fillna(0)

    # 6. Calcul de la répétition moyenne
    df_final["repetition_moyenne_tweets"] = round(df_final["nb_occurrences"] / df_final["nb_total_tweets"],2)

    # 7. Supprimer les colonnes intermédiaires
    df_final = df_final[["UserID", "repetition_moyenne_tweets"]]

    return df_final




def calculate_time_between_tweets(df):
    df["CreatedAt"] = pd.to_datetime(df["CreatedAt"])
    df.sort_values(by=["UserID", "CreatedAt"], inplace=True)
    df["temps_entre_tweets"] = df.groupby("UserID")["CreatedAt"].diff().dt.total_seconds()
    return df.groupby("UserID")["temps_entre_tweets"].agg(["mean", "max"])

# def calculate_time_between_tweets(df):
#     df["CreatedAt"] = pd.to_datetime(df["CreatedAt"], errors='coerce')
#     df = df.sort_values(by=["UserID", "CreatedAt"])
    
#     df["temps_entre_tweets"] = df.groupby("UserID")["CreatedAt"].diff().dt.total_seconds()
    
#     return df.groupby("UserID")["temps_entre_tweets"].agg(["mean", "max"]).reset_index()



#Calcule le nombre moyen de tweets par jour."""
def calculer_tweets_par_jour(nombre_tweets, date_creation):
    duree_vie_compte = calculer_duree_compte(date_creation)
    return round(nombre_tweets / duree_vie_compte, 2) if duree_vie_compte > 0 else 0



#Calcule la durée d'existence d'un compte Twitter.
# Retourne le nombre de jours depuis la création du compte
def calculer_duree_compte(created_at):

    if pd.isna(created_at):
        return 0  # Si la date est manquante, retourner 0

    # Vérifier si la date est déjà un objet datetime
    if isinstance(created_at, str):
        try:
            created_at = pd.to_datetime(created_at, errors='coerce')  # Convertir la chaîne en datetime
        except Exception:
            return 0  # Retourner 0 en cas d'erreur

    # Calcul du nombre de jours
    jours_ecoules = (pd.Timestamp.now() - created_at).days
    return max(jours_ecoules, 1)  # Retourner au moins 1 pour éviter la division par 0

#message d'erreur dans un fichier log 
def write_log(message, log_file="log_erreurs.txt"):
   
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_message = f"[{timestamp}] {message}\n"

    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_message)
        print(f"⚠️ Erreur enregistrée dans {log_file}: {message}")
    except Exception as e:
        print(f"Impossible d'écrire dans le fichier log : {e}")


