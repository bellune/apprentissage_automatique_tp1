import pandas as pd
import functions as fs
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

def algo_arbre_decision(train_file,test_file):
    # 1. Charger les fichiers CSV (Assurez-vous que les chemins sont corrects)
   

    df_train = pd.read_csv(train_file)
    df_test = pd.read_csv(test_file)

    print(f"Fichiers chargés : {df_train.shape[0]} train / {df_test.shape[0]} test.")

    # 2. Séparer les variables (X = features, y = labels)
    X_train = df_train.drop(columns=["Classe"])  # Suppression de la colonne cible
    y_train = df_train["Classe"]                 # Colonne cible

    X_test = df_test.drop(columns=["Classe"])
    y_test = df_test["Classe"]

    print(f"Données d'entraînement : {X_train.shape} | Données de test : {X_test.shape}")

    # 3. Créer et entraîner l’arbre de décision
    model = DecisionTreeClassifier(random_state=42)
    model.fit(X_train, y_train)
    print("Modèle d'arbre de décision entraîné avec succès.")

    # 4. Prédictions sur l’ensemble de test
    y_pred = model.predict(X_test)

    # 5. Évaluation du modèle
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    conf_matrix = confusion_matrix(y_test, y_pred)

    # 6. Afficher les métriques de performance
    fs.log_result(f"------------Arbre de decision---------------------")

    fs.log_result(f"Précision du modèle : {accuracy:.4f}")
    fs.log_result("Rapport de classification :\n", report)
    fs.log_result("Matrice de confusion :\n", conf_matrix)



def execute_algo():

    train_file = "Pretraitement/train/training_data.csv"
    test_file = "Pretraitement/test/test_data.csv"


# 1- Arbre de décision
    algo_arbre_decision(train_file,test_file)

execute_algo()