import os
import mlflow
import mlflow.spark


from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
gold_path = os.path.join(BASE_DIR, "data", "gold")
model_path = os.path.join(BASE_DIR, "models", "churn_lr_pipeline")

spark = SparkSession.builder \
    .appName("ChurnModelTraining") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

mlflow.set_experiment("churn_prediction_experiment")


with mlflow.start_run(run_name="logreg_threshold_0_4"):
    df_gold = spark.read.json(gold_path)

    print("Schema of the gold data")
    df_gold.printSchema()

    df_gold_labeled = df_gold.withColumn(
        "churn_label",
        when(
            (
                (col("late_payments") >= 3) &
                (col("support_tickets") >= 7)
            ) |
            (
                (col("tenure_months") < 6) &
                (col("monthly_charges") > 100)
            ) |
            (
                (col("subscription_type") == "Basic") &
                (col("support_tickets") >= 5)
            ),
            1
        ).otherwise(0)
    )

    label_counts = df_gold_labeled.groupBy("churn_label").count().collect()

    counts = {row['churn_label']: row['count'] for row in label_counts}

    neg = counts.get(0.0, 1)
    pos = counts.get(1.0, 1)

    balance_ratio = neg / pos

    threshold = .4
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("maxIter", 20)
    mlflow.log_param("threshold", threshold)
    mlflow.log_param("balance_ratio", balance_ratio)

    print(f"Balance ratio (neg/pos): {balance_ratio}")

    df_weighted = df_gold_labeled.withColumn(
        "class_weight",
        when(col("churn_label") == 1, balance_ratio).otherwise(1.0)
    )

    categorical_cols = [
        "gender",
        "location",
        "subscription_type",
        "tenure_bucket",
        "charge_bucket"
    ]

    numeric_cols = [
        "monthly_charges",
        "tenure_months",
        "support_tickets",
        "late_payments"
    ]

    indexers = [StringIndexer(
        inputCol=c, outputCol=f"{c}_index", handleInvalid="keep") for c in categorical_cols]

    encoders = [
        OneHotEncoder(inputCol=f"{c}_index", outputCol=f"{c}_ohe") for c in categorical_cols
    ]

    feature_cols = [f"{c}_ohe" for c in categorical_cols] + numeric_cols

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="churn_label",
        weightCol="class_weight",
        maxIter=20
    )

    pipeline = Pipeline(stages=indexers + encoders + [assembler, lr])

    train_df, test_df = df_weighted.randomSplit([.8, .2], seed=42)

    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)

    predictions_custom = predictions.withColumn(
        "churn_prob",
        vector_to_array(col("probability"))[1]
    ).withColumn(
        "prediction_custom",
        when(col("churn_prob") >= threshold, 1.0).otherwise(0.0)
    )

    predictions_custom.groupBy(
        "churn_label", "prediction_custom").count().show()

    cm = predictions_custom.select(
        when((col("churn_label") == 1) & (
            col("prediction_custom") == 1), 1).otherwise(0).alias("tp"),
        when((col("churn_label") == 0) & (
            col("prediction_custom") == 0), 1).otherwise(0).alias("tn"),
        when((col("churn_label") == 0) & (
            col("prediction_custom") == 1), 1).otherwise(0).alias("fp"),
        when((col("churn_label") == 1) & (
            col("prediction_custom") == 0), 1).otherwise(0).alias("fn")
    ).agg(
        spark_sum("tp").alias("TP"),
        spark_sum("tn").alias("TN"),
        spark_sum("fp").alias("FP"),
        spark_sum("fn").alias("FN")
    ).collect()[0]

    TP, TN, FP, FN = cm["TP"], cm["TN"], cm["FP"], cm["FN"]

    accuracy = (TP + TN) / (TP + TN + FP +
                            FN) if (TP + TN + FP + FN) > 0 else 0.0
    precision = TP / (TP + FP) if (TP + FP) > 0 else 0.0
    recall = TP / (TP + FN) if (TP + FN) > 0 else 0.0
    f1_score = 2 * (precision * recall) / (precision +
                                           recall) if (precision + recall) > 0 else 0.0
    specificity = TN / (TN + FP) if (TN + FP) > 0 else 0.0

    print(f"Final threshold: {threshold}")
    print(f"TP={TP}, TN={TN}, FP={FP}, FN={FN}")
    print(f"Accuracy={accuracy:.4f}")
    print(f"Precision(class 1)={precision:.4f}")
    print(f"Recall(class 1)={recall:.4f}")
    print(f"F1(class 1)={f1_score:.4f}")
    print(f"Specificity={specificity:.4f}")

    roc_evaluator = BinaryClassificationEvaluator(
        labelCol="churn_label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    pr_evaluator = BinaryClassificationEvaluator(
        labelCol="churn_label",
        rawPredictionCol='rawPrediction',
        metricName="areaUnderPR"
    )

    roc_auc = roc_evaluator.evaluate(predictions)
    pr_auc = pr_evaluator.evaluate(predictions)

    print(f"ROC AUC:{roc_auc:.4f}")
    print(f"PR AUC:{pr_auc:.4f}")

    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision_class_1", precision)
    mlflow.log_metric("recall_class_1", recall)
    mlflow.log_metric("f1_class_1", f1_score)
    mlflow.log_metric("specificity", specificity)

    mlflow.log_metric("roc_auc", roc_auc)
    mlflow.log_metric("pr_auc", pr_auc)

    mlflow.log_metric("TP", TP)
    mlflow.log_metric("TN", TN)
    mlflow.log_metric("FP", FP)
    mlflow.log_metric("FN", FN)

    model.write().overwrite().save(model_path)
    print(f"Model saved to {model_path}")

    mlflow.log_param("model_path", model_path)
    mlflow.spark.log_model(model, "model")

spark.stop()
