import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OrdinalEncoder
from pprint import pprint
encoder = OrdinalEncoder(
    handle_unknown="use_encoded_value",
    unknown_value=-1
)



df = pd.read_parquet("dataset.parquet")

X = df.drop("Credit_Score", axis=1)
y = df["Credit_Score"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
# X_train[["Occupation"]] = encoder.fit_transform(X_train[["Occupation"]])
# X_test[["Occupation"]] = encoder.transform(X_test[["Occupation"]])

model = RandomForestClassifier(n_estimators=100,max_depth=10,
                               random_state=42,criterion='gini', 
                               n_jobs=-1,max_features='sqrt',bootstrap=True,
                               class_weight="balanced_subsample")


model.fit(X_train, y_train)
y_pred = model.predict(X_test)
print("Accuracy:", accuracy_score(y_test, y_pred))
print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred))