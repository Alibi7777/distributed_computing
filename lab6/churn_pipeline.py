#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    StandardScaler,
)
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--input",
        default="hdfs:///user/hadoop/churn_input/Churn_Modelling.csv",
        help="HDFS path to CSV",
    )
    p.add_argument(
        "--experiment",
        choices=["none", "ablation", "model_compare"],
        default="none",
        help="Experiment type",
    )
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--rf_trees", type=int, default=100)
    return p.parse_args()


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("CustomerChurnPipeline_EMR").getOrCreate()

    # --- 1) Load data ---
    df = spark.read.csv(args.input, header=True, inferSchema=True)

    # Minimal cleanup: drop rows with nulls in used columns
    used_cols = [
        "Exited",
        "Geography",
        "Gender",
        "CreditScore",
        "Age",
        "Tenure",
        "Balance",
        "NumOfProducts",
        "EstimatedSalary",
    ]
    df = df.select(*used_cols).na.drop()

    # Make sure label is numeric
    df = df.withColumn("Exited", F.col("Exited").cast("double"))

    # Split train/test
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=args.seed)

    # --- 2–4) Feature engineering stages ---
    categorical_cols = ["Geography", "Gender"]
    numeric_cols = [
        "CreditScore",
        "Age",
        "Tenure",
        "Balance",
        "NumOfProducts",
        "EstimatedSalary",
    ]

    stages = []

    use_categoricals = args.experiment != "ablation"

    feature_inputs = list(numeric_cols)

    if use_categoricals:
        geo_indexer = StringIndexer(
            inputCol="Geography", outputCol="GeographyIndex", handleInvalid="keep"
        )
        gen_indexer = StringIndexer(
            inputCol="Gender", outputCol="GenderIndex", handleInvalid="keep"
        )

        encoder = OneHotEncoder(
            inputCols=["GeographyIndex", "GenderIndex"],
            outputCols=["GeographyVec", "GenderVec"],
            handleInvalid="keep",
        )

        stages += [geo_indexer, gen_indexer, encoder]
        feature_inputs += ["GeographyVec", "GenderVec"]

    assembler = VectorAssembler(inputCols=feature_inputs, outputCol="features")
    scaler = StandardScaler(
        inputCol="features", outputCol="scaledFeatures", withMean=True, withStd=True
    )

    stages += [assembler, scaler]

    # --- 5) Model stage(s) ---
    lr = LogisticRegression(
        labelCol="Exited",
        featuresCol="scaledFeatures",
        maxIter=50,
        regParam=0.0,
        elasticNetParam=0.0,
    )

    if args.experiment == "model_compare":
        rf = RandomForestClassifier(
            labelCol="Exited",
            featuresCol="scaledFeatures",
            numTrees=args.rf_trees,
            seed=args.seed,
        )
        # We'll train two separate pipelines and compare
        models_to_run = [("LogReg", lr), ("RandomForest", rf)]
    else:
        models_to_run = [("LogReg", lr)]

    # Evaluators
    auc_eval = BinaryClassificationEvaluator(
        labelCol="Exited", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    acc_eval = MulticlassClassificationEvaluator(
        labelCol="Exited", predictionCol="prediction", metricName="accuracy"
    )
    f1_eval = MulticlassClassificationEvaluator(
        labelCol="Exited", predictionCol="prediction", metricName="f1"
    )

    print("\n=== DATA INFO ===")
    print("Train count:", train_df.count())
    print("Test count :", test_df.count())
    print("Experiment :", args.experiment)

    results = []

    for name, clf in models_to_run:
        pipeline = Pipeline(stages=stages + [clf])

        print(f"\n=== TRAINING: {name} ===")
        model = pipeline.fit(train_df)

        print(f"=== PREDICTING: {name} ===")
        pred = model.transform(test_df)

        # Show sample predictions
        pred.select("Exited", "prediction", "probability").show(10, truncate=False)

        auc = auc_eval.evaluate(pred)
        acc = acc_eval.evaluate(pred)
        f1 = f1_eval.evaluate(pred)

        results.append((name, auc, acc, f1))

    print("\n=== RESULTS (Test Set) ===")
    print("Model\t\tAUC\t\tAccuracy\tF1")
    for r in results:
        print(f"{r[0]:<12}\t{r[1]:.4f}\t\t{r[2]:.4f}\t\t{r[3]:.4f}")

    # If ablation: print reminder what changed
    if args.experiment == "ablation":
        print("\n[Ablation] Categorical features removed: Geography, Gender")
        print("Compare metrics/runtime vs baseline run.")

    spark.stop()


if __name__ == "__main__":
    main()
