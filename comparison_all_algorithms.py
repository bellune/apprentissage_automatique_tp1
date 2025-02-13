import os
import pandas as pd
import functions as fs
import matplotlib.pyplot as plt
from sklearn.ensemble import AdaBoostClassifier, BaggingClassifier
from sklearn.tree import DecisionTreeClassifier
import seaborn as sns
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
import matplotlib.pyplot as plt
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
                                                    n_estimators=50, learning_rate=1.0, algorithm="SAMME",random_state=42),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, max_depth=3),
        "Random Forest": RandomForestClassifier(n_estimators=100, max_depth=3),
        "Naives baysien":  GaussianNB() }
        

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
            # Matrice de confusion détaillée
        conf_matrix = confusion_matrix(y_test, y_pred)

        print("\n📝 Matrice de confusion :")
        print(conf_matrix)  # Afficher la matrice

        tn, fp, fn, tp = conf_matrix.ravel()

        

         # Calcul des métriques UNIQUEMENT POUR LES POLLUEURS (Classe = 1)
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0  # Précision
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0  # Rappel (déjà TP Rate)
        fp_rate = fp / (fp + tn) if (fp + tn) > 0 else 0  # Taux de faux positifs
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0  # F-mesure
        auc_score = roc_auc_score(y_test, y_prob)  

        # Stocker les résultats
        results.append({
                    "Modele": name,
                    "Precision (Pollueurs)": round(precision, 4),
                    "Rappel (Pollueurs)": round(recall, 4),
                    "FP Rate (Pollueurs)": round(fp_rate, 4),
                    "F-mesure (Pollueurs)": round(f1_score, 4),
                    "AUC ROC ": round(auc_score, 4)
        })

        # Tracer la courbe ROC pour ce modèle
        fpr, tpr, _ = roc_curve(y_test, y_prob)
        plt.plot(fpr, tpr, label=f'{name} (AUC = {auc_score:.4f})')

        # Exemple d'utilisation après l'entraînement d'un modèle
        plot_confusion_matrix(y_test, y_pred, name, title)

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
    title = 'Données Equilibrées'
    all_algorithme(train_file,test_file,result_file,title)

    # Entrainement des donnees desequilibrees
    train_file = "Pretraitement/train/training_unbalance_data.csv"
    test_file = "Pretraitement/test/test_unbalance_data.csv"
    result_file = 'comparaison_modele_unbalance.csv'
    title = 'Comparaison des courbes ROC - Données Déséquilibrées'
    all_algorithme(train_file,test_file,result_file,title)


def get_courve(title,plt, save_folder="resultats/graphes"):

     # Créer le dossier s'il n'existe pas
    os.makedirs(save_folder, exist_ok=True)

    file_name = f"roc_curve_{title.replace(' ', '_')}.png"
    file_path = os.path.join(save_folder, file_name)

    # Tracer la courbe ROC finale
    plt.plot([0, 1], [0, 1], color='gray', linestyle='--')  # Diagonale
    plt.xlabel('Taux de Faux Positifs (FPR)')
    plt.ylabel('Taux de Vrais Positifs (TPR)')
    plt.title('Comparaison des courbes ROC - '+title)
    plt.legend()
    plt.grid()
    # plt.show()
    # Sauvegarder l'image
    plt.savefig(file_path, bbox_inches="tight", dpi=300)
    plt.close()  # Fermer la figure pour économiser la mémoire

    print(f"Courbe ROC sauvegardée : {file_path}")




# Fonction pour tracer et enregistrer la matrice de confusion
def plot_confusion_matrix(y_test, y_pred, model_name, title, save_folder="resultats/graphes"):
    # Créer le dossier s'il n'existe pas
    os.makedirs(save_folder, exist_ok=True)

    # Générer la matrice de confusion
    cm = confusion_matrix(y_test, y_pred)

    # Tracer la matrice sous forme de carte de chaleur
    plt.figure(figsize=(6, 5))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues",
                xticklabels=["Légitimes (0)", "Pollueurs (1)"], 
                yticklabels=["Légitimes (0)", "Pollueurs (1)"])
    
    plt.xlabel("Prédictions")
    plt.ylabel("Classes")
    plt.title(f"Matrice de Confusion - {model_name}")

    # Définir le chemin du fichier image
    file_path = os.path.join(save_folder, f"conf_matrix_{title}_{model_name}.png")

    # Sauvegarder l'image
    plt.savefig(file_path, bbox_inches="tight", dpi=300)
    plt.close()  # Fermer la figure pour économiser la mémoire

    print(f"Matrice de confusion sauvegardée : {file_path}")




