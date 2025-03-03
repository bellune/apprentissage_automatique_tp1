from sklearn.ensemble import AdaBoostClassifier, BaggingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.naive_bayes import GaussianNB, MultinomialNB

#Donnees Equilibrees
models = {
        "Decision Tree": DecisionTreeClassifier(random_state=42,max_depth=10, min_samples_split=2,min_samples_leaf=35),
        "Bagging": BaggingClassifier(estimator=DecisionTreeClassifier(max_depth=5, min_samples_leaf=20),
                                                  n_estimators=100,
                                                    random_state=42),
        "AdaBoost": AdaBoostClassifier(estimator=DecisionTreeClassifier(max_depth=5, min_samples_split=2,min_samples_leaf=20),
                                                    n_estimators=100,
                                                    learning_rate=1.0, 
                                                    algorithm="SAMME",
                                                    random_state=42),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100,
                                                         learning_rate=0.1,
                                                           max_depth=5),
        "Random Forest": RandomForestClassifier(n_estimators=100, max_depth=5),
        "Naives baysien":  MultinomialNB(alpha=0.1, fit_prior=False)
          }

#Données Déséquilibrées
models_unbalance = {
        "Decision Tree": DecisionTreeClassifier(random_state=42,max_depth=20,class_weight="balanced", min_samples_leaf=5),
        "Bagging": BaggingClassifier(estimator=DecisionTreeClassifier(max_depth=10, min_samples_leaf=20,class_weight="balanced"),
                                                  n_estimators=200,
                                                  random_state=42),
        "AdaBoost": AdaBoostClassifier(estimator=DecisionTreeClassifier(max_depth=1, min_samples_leaf=3),
                                                    n_estimators=200,
                                                    learning_rate=1.0,
                                                    algorithm="SAMME",
                                                    random_state=42),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=50,
                                                        learning_rate=0.1,
                                                         max_depth=3),
        "Random Forest": RandomForestClassifier(n_estimators=200,
                                                class_weight="balanced_subsample",
                                                max_depth=15,
                                                min_samples_leaf=5, 
                                                bootstrap=True,
                                                random_state=42
                                                 ),
        "Naives baysien":  GaussianNB()
        # "Naives baysien":  MultinomialNB(alpha=0.1, fit_prior=False)
        
        }