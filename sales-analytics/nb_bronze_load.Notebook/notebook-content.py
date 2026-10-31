# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cd7d666e-33c5-4bed-8fe7-8512e9bbaa34",
# META       "default_lakehouse_name": "SalesLakehouse",
# META       "default_lakehouse_workspace_id": "44eaebf6-c640-4af4-a272-8dd8d577247b",
# META       "known_lakehouses": [
# META         {
# META           "id": "cd7d666e-33c5-4bed-8fe7-8512e9bbaa34"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# parameters
p_run_id = ""
p_landing_path = "Files/landing"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
import uuid, json, datetime as dt

spark.conf.set("spark.sql.session.timeZone", "UTC")

RUN_ID = p_run_id or ("bronze-" + dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6])
LANDING = p_landing_path
SOURCES = [
    ("customer", "bronze_customer"),
    ("product", "bronze_product"),
    ("order", "bronze_order"),
    ("order_line", "bronze_order_line"),
]
print("run_id:", RUN_ID, "landing:", LANDING)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_run(stage, status, table_name, row_count, source_file, note=""):
    schema = ("run_id string, stage string, status string, table_name string, "
              "row_count long, source_file string, note string, logged_at timestamp")
    rows = [(RUN_ID, stage, status, table_name, int(row_count), source_file, note, dt.datetime.utcnow())]
    (spark.createDataFrame(rows, schema)
          .write.mode("append").option("mergeSchema", "true")
          .format("delta").saveAsTable("ops_notebook_runs"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bronze is a full snapshot of the landing files: every column is kept as the raw
# string exactly as it appears in the CSV (nullValue sentinel prevents empty
# strings from being coerced to NULL), and ingestion metadata is appended.
# Because each run replaces the table (overwrite), reruns never duplicate rows.
summary = {}
failures = []

for src, target in SOURCES:
    path = LANDING + "/" + src + ".csv"
    raw = (spark.read
           .option("header", True)
           .option("inferSchema", False)
           .option("nullValue", "\u0000")
           .option("emptyValue", "")
           .csv(path))
    source_lines = spark.read.text(path).count() - 1

    df = (raw
          .withColumn("_source_file", F.lit(src + ".csv"))
          .withColumn("_source_path", F.lit(path))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_ingest_run_id", F.lit(RUN_ID)))

    (df.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .format("delta").saveAsTable(target))

    rows = spark.table(target).count()
    match = (rows == source_lines)
    summary[target] = {"source_lines": source_lines, "bronze_rows": rows, "match": match}
    log_run("bronze", "Succeeded" if match else "Failed", target, rows,
            src + ".csv", "source_lines=" + str(source_lines))
    if not match:
        failures.append(target)
    print(target, "source_lines=", source_lines, "bronze_rows=", rows, "match=", match)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if failures:
    raise Exception("Bronze row count vs source mismatch: " + ", ".join(failures))
mssparkutils.notebook.exit(json.dumps(
    {"stage": "bronze", "run_id": RUN_ID, "status": "Succeeded", "tables": summary}))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
