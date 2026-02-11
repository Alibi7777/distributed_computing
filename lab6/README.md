# LAB 6 — Spark ML Pipeline on Amazon EMR (Customer Churn)

## Dataset
Kaggle: Churn Modelling (Churn_Modelling.csv)  
Target: Exited (0/1)

## Upload to HDFS
```bash
hdfs dfs -mkdir -p /user/hadoop/churn_input
hdfs dfs -put -f Churn_Modelling.csv /user/hadoop/churn_input/
hdfs dfs -ls /user/hadoop/churn_input
