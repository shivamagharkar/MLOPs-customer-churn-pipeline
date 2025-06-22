# MLOPs-customer-churn-pipeline
Created the entire pipeline using snowflake+.dbt+modelling orchestrated using airflow
• Built a complete MLOps pipeline to predict customer churn using a synthetic dataset, automating data ingestion, transformation, training, and deployment.

• Ingested CSV data into Snowflake using Python scripts and the Snowflake connector, establishing a structured data warehouse.

• Designed and implemented dbt models to transform raw data into clean staging and mart layers, applying SQL-based data modeling best practices.

• Developed a logistic regression model using Scikit-learn with preprocessing and feature engineering pipelines; logged predictions back to Snowflake.

• Orchestrated the full workflow using Apache Airflow (via Astronomer), managing dependencies between data loading, transformation, and model execution tasks.

• Integrated CI/CD with GitHub Actions to automate testing of dbt models and ML training scripts on every push to the `main` branch.

• Deployed and validated predictions in Snowflake’s RAW schema; applied version control and modular code structure for maintainability and scalability.
