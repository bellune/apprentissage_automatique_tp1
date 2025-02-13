import os
import pandas as pd
import functions as fs
import matplotlib.pyplot as plt
from sklearn.ensemble import AdaBoostClassifier, BaggingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import (
    accuracy_score, classification_report, confusion_matrix, roc_auc_score, roc_curve
)


def all_algorithme(train_file,test_file, result_file,title,result_folder="resultats/"):


    # 1. Charger les fichiers CSV
   

    df_train = pd.read_csv(train_file)
    df_test = pd.read_csv(test_file)

    # 2. Séparer les variables X (features) et y (classe)
    X_train = df_train.drop(columns=["Classe"])
    y_train = df_train["Classe"]

    X_test = df_test.drop(columns=["Classe"])
    y_test = df_test["Classe"]

    # 3. Définir les modèles
    models = {
        "Decision Tree": DecisionTreeClassifier(random_state=42),
        "Bagging (50 arbres)": BaggingClassifier(estimator=DecisionTreeClassifier(),
                                                  n_estimators=50, random_state=42),
        "AdaBoost (50 arbres)": AdaBoostClassifier(estimator=DecisionTreeClassifier(max_depth=1),
                                                    n_estimators=50, learning_rate=1.0, algorithm="SAMME",random_state=42) }

    # 4. Entraîner et évaluer chaque modèle
    results = []
    plt.figure(figsize=(8, 6))  # Préparer une figure pour la courbe ROC

    for name, model in models.items():
        print(f"\nEntraînement du modèle : {name}")
        
        # Entraînement
        model.fit(X_train, y_train)
        
        # Prédiction
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]  # Probabilités pour l'AUC ROC
        
        # Matrice de confusion
        tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()

        # Calcul des métriques demandées
        tp_rate = tp / (tp + fn) if (tp + fn) > 0 else 0  # Taux de vrais positifs
        fp_rate = fp / (fp + tn) if (fp + tn) > 0 else 0  # Taux de faux positifs
        f1_score = 2 * tp / (2 * tp + fp + fn) if (2 * tp + fp + fn) > 0 else 0  # F-mesure
        auc_score = roc_auc_score(y_test, y_prob)  # AUC ROC

        # Stocker les résultats
        results.append({
            "Model": name,
            "TP Rate (Taux de vrais positifs)": round(tp_rate, 4),
            "FP Rate (Taux de faux positifs)": round(fp_rate, 4),
            "F-mesure": round(f1_score, 4),
            "AUC ROC": round(auc_score, 4)
        })

        # Tracer la courbe ROC pour ce modèle
        fpr, tpr, _ = roc_curve(y_test, y_prob)
        plt.plot(fpr, tpr, label=f'{name} (AUC = {auc_score:.4f})')

    # 5. Convertir en DataFrame pour afficher proprement
    df_results = pd.DataFrame(results)

   

    # 6. Afficher les résultats
    # Sauvegarde finale
    csv_path = os.path.join(result_folder, result_file)
    csv_folder = os.path.dirname(csv_path)

    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)

    df_results.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"Résultats enregistrés : {csv_path}")
    get_courve(title,plt)




def execute_algo():
    # Entrainement des donnees
    train_file = "Pretraitement/train/training_data.csv"
    test_file = "Pretraitement/test/test_data.csv"
    result_file = 'comparaison_modele.csv'
    title = 'Comparaison des courbes ROC - Données Equilibrées'
    all_algorithme(train_file,test_file,result_file,title)

    # Entrainement des donnees desequilibrees
    train_file = "Pretraitement/train/training_unbalance_data.csv"
    test_file = "Pretraitement/test/test_unbalance_data.csv"
    result_file = 'comparaison_modele_unbalance.csv'
    title = 'Comparaison des courbes ROC - Données Déséquilibrées'
    all_algorithme(train_file,test_file,result_file,title)


def get_courve(title,plt):
    # Tracer la courbe ROC finale
    plt.plot([0, 1], [0, 1], color='gray', linestyle='--')  # Diagonale
    plt.xlabel('Taux de Faux Positifs (FPR)')
    plt.ylabel('Taux de Vrais Positifs (TPR)')
    plt.title(title)
    plt.legend()
    plt.grid()
    plt.show()

