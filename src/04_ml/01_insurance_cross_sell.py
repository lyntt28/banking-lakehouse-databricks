# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - ML: bán chéo bảo hiểm (propensity) - feature table, train, MLflow, chấm điểm
# MAGIC 1. Feature 1 dòng / KH cá nhân / ngày từ Silver -> `gold.feature_customer_insurance`.
# MAGIC 2. Nhãn: `label_mode=holder` (đang có HĐ INFORCE) hoặc `future` (mua mới trong
# MAGIC    `horizon_days` sau as_of; cần lịch sử, chọn as_of cũ hơn ngày mới nhất >= horizon).
# MAGIC 3. Train HistGradientBoosting, log MLflow (ROC-AUC, PR-AUC, lift top 10%),
# MAGIC    đăng ký model Unity Catalog `<catalog>.gold.insurance_propensity`.
# MAGIC 4. Chấm điểm KH CHƯA có HĐ -> `gold.insurance_propensity_scores` (danh sách cho telesale).

# COMMAND ----------

# MAGIC %pip install -q scikit-learn==1.5.2 "mlflow-skinny[databricks]"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("as_of_date", "")
dbutils.widgets.dropdown("label_mode", "holder", ["holder", "future"])
dbutils.widgets.text("horizon_days", "90")
dbutils.widgets.dropdown("register", "true", ["true", "false"])
catalog = dbutils.widgets.get("catalog")
as_of_date = dbutils.widgets.get("as_of_date").strip()
label_mode = dbutils.widgets.get("label_mode")
horizon = int(dbutils.widgets.get("horizon_days"))
register = dbutils.widgets.get("register") == "true"

# COMMAND ----------

import os
import sys
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

import mlflow  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.dates import check_date  # noqa: E402
from common.delta_io import overwrite_date  # noqa: E402
from common.governance import assert_pipeline_identity  # noqa: E402
from common.ml_features import (  # noqa: E402
    NUMERIC_FEATURES,
    build_customer_features,
    build_labels,
    train_model,
    training_set,
)

S, G = f"{catalog}.silver", f"{catalog}.gold"
if not as_of_date:
    as_of_date = str(spark.table(f"{S}.loans").agg(F.max("as_of_date")).first()[0])
assert_pipeline_identity(spark, catalog)
as_of_date = check_date(as_of_date)
day = F.lit(as_of_date).cast("date")
print(f"as_of_date={as_of_date} label_mode={label_mode}")

# COMMAND ----------

# 1) Feature table (point-in-time: phiên bản KH có hiệu lực tại as_of)
customers = spark.table(f"{S}.customers").filter(
    (F.col("valid_from") <= day) & (F.col("valid_to").isNull() | (F.col("valid_to") >= day)))
feats = build_customer_features(
    as_of_date, customers, spark.table(f"{S}.accounts"), spark.table(f"{S}.loans"),
    spark.table(f"{S}.cards"), spark.table(f"{S}.card_transactions"),
    spark.table(f"{S}.fx_rates").filter(F.col("rate_date") <= day)
    .groupBy("currencycd").agg(F.max_by("rate_to_vnd", "rate_date").alias("rate_to_vnd")))
overwrite_date(feats, f"{G}.feature_customer_insurance", "as_of_date", as_of_date)
feats = spark.table(f"{G}.feature_customer_insurance").filter(F.col("as_of_date") == day)

# COMMAND ----------

# 2) Train + MLflow
policies = spark.table(f"{S}.insurance_policies")
labels = build_labels(policies, as_of_date, label_mode, horizon)
pdf = training_set(feats, labels, policies, as_of_date, label_mode).toPandas()
print(f"{len(pdf):,} KH, tỷ lệ nhãn 1 = {pdf['label'].mean():.3%}")

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/insurance_cross_sell")
with mlflow.start_run(run_name=f"hgb_{label_mode}_{as_of_date}") as run:
    model, metrics = train_model(pdf)
    mlflow.log_params({"as_of_date": as_of_date, "label_mode": label_mode,
                       "horizon_days": horizon, "n_features": len(NUMERIC_FEATURES)})
    mlflow.log_metrics(metrics)
    sample = pdf[NUMERIC_FEATURES].astype(float).head(5)
    mlflow.sklearn.log_model(
        model, "model", input_example=sample,
        registered_model_name=f"{G}.insurance_propensity" if register else None)
print(metrics)

# COMMAND ----------

# 3) Chấm điểm KH chưa có hợp đồng hiệu lực -> danh sách ưu tiên cho telesale
holders = build_labels(policies, as_of_date, "holder").select("customer_id")
cand = feats.join(holders, "customer_id", "left_anti").toPandas()
cand["score"] = model.predict_proba(cand[NUMERIC_FEATURES].astype(float))[:, 1]
scores = spark.createDataFrame(cand[["customer_id", "segment", "province", "score"]]) \
    .withColumn("as_of_date", day) \
    .withColumn("score_decile", F.ntile(10).over(Window.orderBy(F.desc("score")))) \
    .withColumn("model_run_id", F.lit(run.info.run_id))
overwrite_date(scores, f"{G}.insurance_propensity_scores", "as_of_date", as_of_date)
display(spark.table(f"{G}.insurance_propensity_scores")
        .filter(F.col("as_of_date") == day).orderBy(F.desc("score")).limit(20))
