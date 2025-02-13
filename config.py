from sklearn.ensemble import AdaBoostClassifier, BaggingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.naive_bayes import GaussianNB

#Donnees Equilibrees
models = {
        "Decision Tree": DecisionTreeClassifier(random_state=42),
        "Bagging (50 arbres)": BaggingClassifier(estimator=DecisionTreeClassifier(),
                                                  n_estimators=50, random_state=42),
        "AdaBoost (50 arbres)": AdaBoostClassifier(estimator=DecisionTreeClassifier(max_depth=1),
                                                    n_estimators=50, learning_rate=1.0, algorithm="SAMME",random_state=42),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, max_depth=3),
        "Random Forest": RandomForestClassifier(n_estimators=100, max_depth=3),
        "Naives baysien":  GaussianNB() }

models_unbalance = {
        "Decision Tree": DecisionTreeClassifier(random_state=42),
        "Bagging (50 arbres)": BaggingClassifier(estimator=DecisionTreeClassifier(),
                                                  n_estimators=50, random_state=42),
        "AdaBoost (50 arbres)": AdaBoostClassifier(estimator=DecisionTreeClassifier(max_depth=1),
                                                    n_estimators=50, learning_rate=1.0, algorithm="SAMME",random_state=42),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, max_depth=3),
        "Random Forest": RandomForestClassifier(n_estimators=100, max_depth=3),
        "Naives baysien":  GaussianNB() }