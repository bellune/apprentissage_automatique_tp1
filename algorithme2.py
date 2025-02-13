import pandas as pd
import numpy as np
import functions as fs
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import roc_curve
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import roc_curve, auc, precision_score, recall_score, f1_score, confusion_matrix, roc_auc_score

#  Fonction pour charger les données depuis un fichier CSV
def load_data(train_file, test_file, target_column):
    """
    Charge les fichiers CSV et sépare les features (X) et la cible (y).
    
    :param train_file: Chemin du fichier CSV d'entraînement
    :param test_file: Chemin du fichier CSV de test
    :param target_column: Nom de la colonne cible
    :return: X_train, y_train, X_test, y_test
    """
    train_df = pd.read_csv(train_file)
    test_df = pd.read_csv(test_file)

    X_train, y_train = train_df.drop(columns=[target_column]), train_df[target_column]
    X_test, y_test = test_df.drop(columns=[target_column]), test_df[target_column]

    return X_train, y_train, X_test, y_test

#  Fonction pour entraîner un modèle Gradient Boosting
def train_gboost(X_train, y_train, n_estimators=100, learning_rate=0.1, max_depth=3):
    model = GradientBoostingClassifier(n_estimators=n_estimators, learning_rate=learning_rate, max_depth=max_depth)
    model.fit(X_train, y_train)
    return model

#  Fonction pour entraîner un modèle Random Forest
def train_random_forest(X_train, y_train, n_estimators=100, max_depth=3):
    model = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth)
    model.fit(X_train, y_train)
    return model

#  Fonction pour entraîner un modèle Naive Bayes
def train_naive_bayes(X_train, y_train):
    model = GaussianNB()
    model.fit(X_train, y_train)
    return model

#  Fonction pour évaluer un modèle
def evaluate_model(model, X_test, y_test, algorithm_name):
    y_pred = model.predict(X_test)

    # Calcul des métriques
   # accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, pos_label=1)  # Focus sur les pollueurs (classe 1)
    recall = recall_score(y_test, y_pred, pos_label=1)  # Rappel : combien de pollueurs détectés ?
    f1 = f1_score(y_test, y_pred, pos_label=1)  # Score F1 (équilibre entre précision et rappel)
     # Matrice de confusion : [[TN, FP], [FN, TP]]
    conf_matrix = confusion_matrix(y_test, y_pred)
    tn, fp, fn, tp = conf_matrix.ravel()
    
     # Calcul du taux de faux positifs (FP Rate = 1 - Spécificité)
    fp_rate = fp / (fp + tn) if (fp + tn) > 0 else 0
   

    # Calcul de l'AUC-ROC si applicable (classification binaire)
   # AUC-ROC
    auc_roc = None
    fpr, tpr, roc_auc = None, None, None
    
    if len(np.unique(y_test)) == 2 and hasattr(model, "predict_proba"):
        y_prob = model.predict_proba(X_test)[:, 1]  # Probabilité d'être un pollueur
        fpr, tpr, _ = roc_curve(y_test, y_prob)
        roc_auc = auc(fpr, tpr)
        auc_roc = roc_auc_score(y_test, y_prob)

    # Matrice de confusion
    plt.figure(figsize=(6, 4))
    sns.heatmap(conf_matrix, annot=True, fmt="d", cmap="Blues", xticklabels=np.unique(y_test), yticklabels=np.unique(y_test))
    plt.title(f"Matrice de confusion - {algorithm_name}")
    plt.xlabel("Prédit")
    plt.ylabel("Réel")
    plt.savefig(f"confusion_matrix_{algorithm_name}.png")
    plt.show()

    #  Courbe ROC (si applicable)
    if auc_roc:
        fpr, tpr, _ = roc_curve(y_test, y_prob)
        plt.figure(figsize=(6, 4))
        plt.plot(fpr, tpr, label=f"AUC-ROC = {auc_roc:.4f}")
        plt.plot([0, 1], [0, 1], "k--")  # Diagonale
        plt.xlabel("False Positive Rate")
        plt.ylabel("True Positive Rate")
        plt.title(f"Courbe ROC - {algorithm_name}")
        plt.legend(loc="lower right")
        plt.savefig(f"roc_curve_{algorithm_name}.png")
        plt.show()
    
     # Création d'un tableau Pandas pour afficher les résultats
    results_df = pd.DataFrame({
        "Modèle": [algorithm_name],
        "TP Rate (Recall)": [recall],
        "FP Rate": [fp_rate],
        "Précision": [precision],
        "F1-score": [f1],
        "AUC-ROC": [auc_roc]
    })
      # 6. Afficher les métriques de performance
    fs.log_result(f"------------{algorithm_name} ---------------------")

    fs.log_result(f"\nRésultats pour {algorithm_name} :\n{results_df.to_string(index=False)}")
    
    
    # Affichage des résultats
    print(f" Précision (pollueur) : {precision:.4f}")
    print(f" Matrice de confusion :\n{conf_matrix}")
    if auc_roc is not None:
        print(f" Score AUC-ROC : {auc_roc:.4f}")

    return {
        "name": algorithm_name,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "confusion_matrix": conf_matrix,
        "auc_roc": auc_roc,
        "fpr": fpr,
        "tpr": tpr,
        "roc_auc": roc_auc,
    }
   

#  Fonction principale pour exécuter un algorithme donné avec des fichiers CSV
def run_algorithm_from_csv(train_file, test_file, target_column, algorithm="gboost"):
    """
    Charge les données depuis des fichiers CSV et entraîne un algorithme de classification.
    
    :param train_file: Fichier CSV contenant les données d'entraînement
    :param test_file: Fichier CSV contenant les données de test
    :param target_column: Nom de la colonne cible
    :param algorithm: Type d'algorithme ("gboost", "random_forest", "naive_bayes")
    :return: Résultats de l'évaluation du modèle
    """
    X_train, y_train, X_test, y_test = load_data(train_file, test_file, target_column)

    if algorithm == "gboost":
        model = train_gboost(X_train, y_train)
    elif algorithm == "random_forest":
        model = train_random_forest(X_train, y_train)
    elif algorithm == "naive_bayes":
        model = train_naive_bayes(X_train, y_train)
    else:
        raise ValueError("L'algorithme doit être 'gboost', 'random_forest' ou 'naive_bayes'.")

    return evaluate_model(model, X_test, y_test)
def main():
    # Paramètres
    train_file = "Pretraitement/train/training_data.csv"
    test_file = "Pretraitement/test/test_data.csv"
    target_column = "Classe"

    # Chargement des données
    X_train, y_train, X_test, y_test = load_data(train_file, test_file, target_column)

    # Exécution des algorithmes
    print("\n Exécution de Gradient Boosting...")
    evaluate_model(train_gboost(X_train, y_train), X_test, y_test, "Gradient Boosting")

    print("\n Exécution de Random Forest...")
    evaluate_model(train_random_forest(X_train, y_train), X_test, y_test, "Random Forest")

    print("\n Exécution de Naive Bayes...")
    evaluate_model(train_naive_bayes(X_train, y_train), X_test, y_test, "Naive Bayes")

#Lancement automatique si le fichier est exécuté directement
if __name__ == "__main__":
    main()