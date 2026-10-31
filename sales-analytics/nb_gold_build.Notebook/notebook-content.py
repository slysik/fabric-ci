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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, Window
import uuid, json, datetime as dt

spark.conf.set("spark.sql.session.timeZone", "UTC")

RUN_ID = p_run_id or ("gold-" + dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6])


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


counts = {}
def publish_staging(df, name):
    """Write to a staging table only. Final gold_* names are created by the
    validation notebook, and only when every check passes."""
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .format("delta").saveAsTable(name))
    n = spark.table(name).count()
    counts[name] = n
    log_run("gold", "Staged", name, n, "silver", "awaiting validation")
    print(name, "=", n)
    return n


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Conformed sales fact: order lines enriched with order and product context.
fact = (spark.table("silver_order_line").alias("l")
        .join(spark.table("silver_order").select("order_id", "customer_id", "order_date", "status", "channel", "currency").alias("o"),
              F.col("l.order_id") == F.col("o.order_id"), "inner")
        .join(spark.table("silver_product").select("product_id", "product_name", "category").alias("p"),
              F.col("l.product_id") == F.col("p.product_id"), "inner")
        .select(F.col("l.order_line_id"), F.col("l.order_id"), F.col("l.product_id"),
                F.col("o.customer_id"), F.col("o.order_date"), F.col("o.status"), F.col("o.channel"), F.col("o.currency"),
                F.col("p.product_name"), F.col("p.category"),
                F.col("l.quantity"), F.col("l.unit_price"), F.col("l.discount_pct"),
                F.col("l.gross_revenue"), F.col("l.net_revenue")))
fact = fact.cache()
fact_rows = fact.count()
print("fact rows:", fact_rows)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

daily = (fact.groupBy("order_date")
         .agg(F.countDistinct("order_id").alias("order_count"),
              F.count("order_line_id").alias("line_count"),
              F.sum("quantity").cast("long").alias("units_sold"),
              F.round(F.sum("gross_revenue"), 2).cast("decimal(18,2)").alias("gross_revenue"),
              F.round(F.sum("net_revenue"), 2).cast("decimal(18,2)").alias("net_revenue"))
         .orderBy("order_date"))
publish_staging(daily, "gold_stg_daily_sales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cust = spark.table("silver_customer").select("customer_id", "segment", "country", "first_name", "last_name")
customer_sales = (fact.join(cust, "customer_id", "left")
                  .groupBy("customer_id", "segment", "country", "first_name", "last_name")
                  .agg(F.countDistinct("order_id").alias("order_count"),
                       F.sum("quantity").cast("long").alias("units_sold"),
                       F.round(F.sum("net_revenue"), 2).cast("decimal(18,2)").alias("net_revenue"),
                       F.min("order_date").alias("first_order_date"),
                       F.max("order_date").alias("last_order_date"))
                  .withColumn("customer_name",
                              F.trim(F.concat_ws(" ", F.col("first_name"), F.col("last_name"))))
                  .drop("first_name", "last_name")
                  .orderBy(F.col("net_revenue").desc()))
publish_staging(customer_sales, "gold_stg_customer_sales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product_perf = (fact.groupBy("product_id", "product_name", "category")
                .agg(F.countDistinct("order_id").alias("order_count"),
                     F.sum("quantity").cast("long").alias("units_sold"),
                     F.round(F.sum("net_revenue"), 2).cast("decimal(18,2)").alias("net_revenue"),
                     F.countDistinct("customer_id").alias("buyer_count")))
w = Window.orderBy(F.col("net_revenue").desc())
product_perf = (product_perf.withColumn("revenue_rank", F.row_number().over(w))
                .orderBy("revenue_rank"))
publish_staging(product_perf, "gold_stg_product_performance")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

monthly = (fact.withColumn("revenue_month", F.trunc("order_date", "MM"))
           .join(cust, "customer_id", "left")
           .groupBy("revenue_month", F.col("segment").alias("customer_segment"))
           .agg(F.countDistinct("order_id").alias("order_count"),
                F.sum("quantity").cast("long").alias("units_sold"),
                F.round(F.sum("net_revenue"), 2).cast("decimal(18,2)").alias("net_revenue"))
           .orderBy("revenue_month", "customer_segment"))
publish_staging(monthly, "gold_stg_monthly_revenue_by_segment")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact.unpersist()
mssparkutils.notebook.exit(json.dumps({
    "stage": "gold", "run_id": RUN_ID, "status": "Staged",
    "fact_rows": fact_rows, "counts": counts,
    "note": "gold_* promoted only by nb_validate_promote after all checks pass",
}))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
