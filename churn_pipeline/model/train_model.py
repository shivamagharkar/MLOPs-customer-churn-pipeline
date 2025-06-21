import pandas as pd
import snowflake.connector
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
import joblib
import os
# Connect to Snowflake ---
conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    role=os.environ["SNOWFLAKE_ROLE"]
)

#  Query the features table ---
query = "SELECT * FROM CHURN_FEATURES"
df = pd.read_sql(query, conn)
df.columns = [col.lower() for col in df.columns]
print("Columns:", df.columns.tolist())


# Prepare features & labels ---
drop_cols = ["customer_id", "customerid", "churn_label", "last_interaction", "days_since_last_interaction"]
X = df.drop(columns=[c for c in drop_cols if c in df.columns])

if "churn_label" in df.columns:
    y = df["churn_label"]
elif "churn" in df.columns:
    y = df["churn"]
else:
    raise Exception("Could not find churn target column")
X = X.dropna()
y = y[X.index]
# Identify categorical and numerical columns ---
categorical_cols = ["gender", "subscription_type"]
numerical_cols = [col for col in X.columns if col not in categorical_cols]

# Preprocessing pipeline ---
preprocessor = ColumnTransformer(
    transformers=[
        ("num", "passthrough", numerical_cols),
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_cols),
    ]
)

# Modeling pipeline ---
pipeline = Pipeline(steps=[
    ("preprocessor", preprocessor),
    ("classifier", LogisticRegression(max_iter=1000))
])

# Train/test split ---
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

#Fit the model ---
pipeline.fit(X_train, y_train)

# Evaluate ---
y_pred = pipeline.predict(X_test)
print("\n Classification Report:\n")
print(classification_report(y_test, y_pred))

# --- 10. Predict on all data ---
X_all = df.drop(columns=["customer_id", "churn_label", "days_since_last_interaction"])
X_all = X_all.dropna()

# Get matching customer_ids
customer_ids = df.loc[X_all.index, "customer_id"]

# Predict probabilities and labels
y_pred = pipeline.predict(X_all)
y_proba = pipeline.predict_proba(X_all)[:, 1]  # churn probability

# Create result DataFrame ---
results = pd.DataFrame({
    "customer_id": customer_ids.astype(str),
    "churn_prediction": y_pred.astype(int),
    "churn_probability": y_proba.astype(float)
})

# Save model (optional) ---
joblib.dump(pipeline, "churn_model.pkl")
print(" Model saved to churn_model.pkl")

from snowflake.connector.pandas_tools import write_pandas
results.columns = [col.upper() for col in results.columns]

# Create table if not exists (in your schema)
conn.cursor().execute("""
    CREATE OR REPLACE TABLE PREDICTED_CHURN (
        customer_id STRING,
        churn_prediction INT,
        churn_probability FLOAT
    )
""")

# Write predictions into the table
success, nchunks, nrows, _ = write_pandas(conn, results, "PREDICTED_CHURN")
print(f"Saved {nrows} predictions to Snowflake table: PREDICTED_CHURN")

