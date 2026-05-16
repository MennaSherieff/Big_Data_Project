# =========================================================
# Member 2 — Big Data Processing (Spark ETL Pipeline)
# CICIDS2017 + UNSW-NB15 Unified Preprocessing
# =========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
import config

# -------------------------
# 1. SPARK SESSION
# -------------------------
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Member2_Spark_Preprocessing") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("hdfs://namenode:9000/data/checkpoints/")

# -------------------------
# 2. LOAD DATA
# -------------------------
# Read the raw Parquet datasets from HDFS as provided by the ingestion pipeline.
unsw_df = spark.read.parquet(config.HDFS_RAW_UNSW)
cic_df  = spark.read.parquet(config.HDFS_RAW_CIC)

print("UNSW rows:", unsw_df.count())
print("CIC rows :", cic_df.count())

# =========================================================
# 3. CLEAN UNSW
# =========================================================
def clean_unsw(df):

    df = df.dropDuplicates()

    numeric_cols = [
        "dur","sbytes","dbytes","sttl","dttl",
        "sloss","dloss","Sload","Dload",
        "Spkts","Dpkts"
    ]

    for c in numeric_cols:
        df = df.withColumn(c, col(c).cast("double"))

    df = df.fillna(0)

    # safe label handling
    df = df.withColumn("Label", trim(col("Label")))

    df = df.withColumnRenamed("dur", "flow_duration") \
           .withColumnRenamed("sbytes", "fwd_bytes") \
           .withColumnRenamed("dbytes", "bwd_bytes") \
           .withColumnRenamed("Spkts", "fwd_pkts") \
           .withColumnRenamed("Dpkts", "bwd_pkts")

    df = df.withColumn(
        "binary_label",
        when(col("Label") == 0, 0).otherwise(1)
    )

    df = df.withColumn(
        "attack_family",
        when(col("attack_cat").isNull(), "Normal")
        .otherwise(col("attack_cat"))
    )

    df = df.withColumn("source_dataset", lit("UNSW"))

    return df


# =========================================================
# 4. CLEAN CICIDS
# =========================================================
def clean_cic(df):

    df = df.dropDuplicates()

    df = df.replace([float("inf"), float("-inf")], None).fillna(0)

    df = df.withColumn("Label", trim(col("Label")))

    df = df.withColumn(
        "flow_duration",
        col("Flow Duration") / 1e6
    ).withColumn(
        "fwd_bytes",
        col("Total Length of Fwd Packets")
    ).withColumn(
        "bwd_bytes",
        col("Total Length of Bwd Packets")
    ).withColumn(
        "fwd_pkts",
        col("Total Fwd Packets")
    ).withColumn(
        "bwd_pkts",
        col("Total Backward Packets")
    )

    df = df.withColumn(
        "binary_label",
        when(lower(col("Label")) == "benign", 0).otherwise(1)
    )

    df = df.withColumn(
        "attack_family",
        when(lower(col("Label")) == "benign", "Normal")
        .otherwise(col("Label"))
    )

    df = df.withColumn("source_dataset", lit("CICIDS"))

    return df


# -------------------------
# 5. APPLY CLEANING
# -------------------------
unsw_clean = clean_unsw(unsw_df)
cic_clean  = clean_cic(cic_df)

# Save intermediate outputs (IMPORTANT for grading/debugging)
unsw_clean.write.mode("overwrite").parquet("hdfs://namenode:9000/data/debug/unsw_clean/")
cic_clean.write.mode("overwrite").parquet("hdfs://namenode:9000/data/debug/cic_clean/")

# -------------------------
# 6. FEATURE ENGINEERING
# -------------------------
def add_features(df):

    df = df.withColumn(
        "total_bytes",
        col("fwd_bytes") + col("bwd_bytes")
    )

    df = df.withColumn(
        "flow_pkts_per_sec",
        when(col("flow_duration") > 0,
             (col("fwd_pkts") + col("bwd_pkts")) / col("flow_duration")
        ).otherwise(0)
    )

    df = df.withColumn(
        "flow_bytes_per_sec",
        when(col("flow_duration") > 0,
             col("total_bytes") / col("flow_duration")
        ).otherwise(0)
    )

    df = df.withColumn(
        "down_up_ratio",
        when(col("fwd_pkts") > 0,
             col("bwd_pkts") / col("fwd_pkts")
        ).otherwise(0)
    )

    return df


unsw_clean = add_features(unsw_clean)
cic_clean  = add_features(cic_clean)

# -------------------------
# 7. UNION DATASETS
# -------------------------
unified_df = unsw_clean.unionByName(cic_clean)

print("Unified rows:", unified_df.count())

unified_df = unified_df.checkpoint()

# -------------------------
# 8. CLASS IMBALANCE HANDLING
# -------------------------
major = unified_df.filter(col("binary_label") == 0)
minor = unified_df.filter(col("binary_label") == 1)

minor_up = minor.sample(True, 1.5)

balanced_df = major.union(minor_up)

# -------------------------
# 9. VECTOR ASSEMBLY (ML READY)
# -------------------------
feature_cols = config.FEATURE_COLS

balanced_df = balanced_df.fillna(0, subset=feature_cols)

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

final_df = assembler.transform(balanced_df)

# -------------------------
# 8.5 EDA CHARTS (Add this section)
# -------------------------
import matplotlib.pyplot as plt
import pandas as pd

print("\nGenerating EDA charts...")

# Create directory for charts
import os
os.makedirs("charts", exist_ok=True)

# 1. Class distribution before balancing
print("  - Class distribution chart")
before_counts = unified_df.groupBy("binary_label").count().toPandas()
before_counts['label'] = before_counts['binary_label'].map({0: 'Normal', 1: 'Attack'})

plt.figure(figsize=(8, 5))
plt.bar(before_counts['label'], before_counts['count'])
plt.title('Class Distribution (Before Balancing)')
plt.ylabel('Count')
plt.savefig('charts/class_distribution_before.png')
plt.close()

# 2. Class distribution after balancing
after_counts = balanced_df.groupBy("binary_label").count().toPandas()
after_counts['label'] = after_counts['binary_label'].map({0: 'Normal', 1: 'Attack'})

plt.figure(figsize=(8, 5))
plt.bar(after_counts['label'], after_counts['count'])
plt.title('Class Distribution (After Balancing)')
plt.ylabel('Count')
plt.savefig('charts/class_distribution_after.png')
plt.close()

# 3. Feature correlation heatmap (sample for performance)
print("  - Feature correlation chart")
feature_sample = balanced_df.select(feature_cols).limit(10000).toPandas()
corr = feature_sample.corr()

plt.figure(figsize=(10, 8))
plt.imshow(corr, cmap='coolwarm', aspect='auto')
plt.colorbar()
plt.xticks(range(len(feature_cols)), feature_cols, rotation=45, ha='right')
plt.yticks(range(len(feature_cols)), feature_cols)
plt.title('Feature Correlation Heatmap')
plt.tight_layout()
plt.savefig('charts/feature_correlation.png')
plt.close()

# 4. Dataset composition
print("  - Dataset composition chart")
dataset_counts = unified_df.groupBy("source_dataset").count().toPandas()

plt.figure(figsize=(8, 5))
plt.pie(dataset_counts['count'], labels=dataset_counts['source_dataset'], autopct='%1.1f%%')
plt.title('Dataset Composition')
plt.savefig('charts/dataset_composition.png')
plt.close()

print("EDA charts saved to 'charts/' directory")


# -------------------------
# 10. SAVE FINAL CLEAN DATASET
# -------------------------
OUTPUT_PATH = config.HDFS_OUTPUT_CLEAN

final_df.select(
    "features",
    "binary_label",
    "source_dataset",
    "attack_family"
).write.mode("overwrite").parquet(OUTPUT_PATH)

print("Clean dataset saved to HDFS:", OUTPUT_PATH)

spark.stop()

# -------------------------
# 11. VERIFY HDFS OUTPUT (Optional)
# -------------------------
print("\nVerifying HDFS output...")
# Read back the saved data to confirm it worked
verify_df = spark.read.parquet(OUTPUT_PATH)
print(f"Verified: {verify_df.count():,} rows saved to HDFS")
print("Schema:")
verify_df.printSchema()
print("\nSample records from HDFS:")
verify_df.select("binary_label", "source_dataset", "attack_family").show(5)