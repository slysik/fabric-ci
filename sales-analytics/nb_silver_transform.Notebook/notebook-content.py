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

# CELL ********************

from pyspark.sql import functions as F, types as T, Window
import uuid, json, datetime as dt

spark.conf.set("spark.sql.session.timeZone", "UTC")

RUN_ID = p_run_id or ("silver-" + dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6])
NOW = F.current_timestamp()

ALLOWED_STATUS = ["completed", "shipped", "pending", "cancelled"]
ALLOWED_SEGMENTS = ["Consumer", "SMB", "Enterprise", "PublicSector"]
ALLOWED_CURRENCY = ["USD", "EUR", "GBP", "CAD", "AUD", "JPY", "BRL"]
ALLOWED_CATEGORIES = ["Electronics", "Home", "Outdoors", "Apparel", "Grocery"]

INT_RE = r"^[0-9]+$"
DEC_RE = r"^[0-9]+(\.[0-9]+)?$"
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

# CELL ********************

def log_run(stage, status, table_name, row_count, source_file, note=""):
    schema = ("run_id string, stage string, status string, table_name string, "
              "row_count long, source_file string, note string, logged_at timestamp")
    rows = [(RUN_ID, stage, status, table_name, int(row_count), source_file, note, dt.datetime.utcnow())]
    (spark.createDataFrame(rows, schema)
          .write.mode("append").option("mergeSchema", "true")
          .format("delta").saveAsTable("ops_notebook_runs"))


def to_quarantine(df, source_table, key_col):
    """Shape rejected rows into the shared quarantine schema."""
    payload_cols = [F.col(c) for c in df.columns if c != "reason"]
    return (df.select(
                F.lit(source_table).alias("source_table"),
                F.col(key_col).cast("string").alias("record_key"),
                F.col("reason"),
                F.to_json(F.struct(*payload_cols)).alias("raw_record"),
                F.col("_source_file").alias("source_file"),
                F.col("_ingested_at").alias("ingested_at"))
              .withColumn("quarantined_at", NOW)
              .withColumn("run_id", F.lit(RUN_ID)))


def is_blank(c):
    """NULL-safe blank test. Plain `col == ""` yields NULL for NULL input, which
    silently skips a when() branch and lets bad rows through."""
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


def not_in(c, allowed):
    return F.col(c).isNull() | ~F.col(c).isin(allowed)


def bad_regex(c, rx):
    return F.col(c).isNull() | ~F.col(c).rlike(rx)


def dedupe(df, keys, order_cols):
    """Keep exactly one row per business key. Deterministic: same rule on every rerun."""
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1).drop("_rn"))


counts = {}
def publish(df, name, stage="silver"):
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .format("delta").saveAsTable(name))
    n = spark.table(name).count()
    counts[name] = n
    log_run(stage, "Succeeded", name, n, "bronze", "silver rebuild")
    print(name, "=", n)
    return n

# CELL ********************

# ---------------- customers: type, validate, dedupe on business key ----------------
c = (spark.table("bronze_customer").select(
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.trim(F.col("first_name")).alias("first_name"),
        F.trim(F.col("last_name")).alias("last_name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.trim(F.col("segment")).alias("segment"),
        F.trim(F.col("country")).alias("country"),
        F.trim(F.col("signup_date")).alias("signup_date_raw"),
        F.lower(F.trim(F.col("status"))).alias("status"),
        F.trim(F.col("updated_at")).alias("updated_at"),
        F.col("_source_file"), F.col("_ingested_at")))
c = c.withColumn("signup_date", F.to_date(F.col("signup_date_raw"), "yyyy-MM-dd"))
c = c.withColumn("reason", F.when(is_blank("customer_id"), "missing_customer_id")
    .when(F.col("signup_date").isNull(), "invalid_signup_date")
    .when(bad_regex("email", EMAIL_RE), "invalid_email")
    .when(not_in("segment", ALLOWED_SEGMENTS), "invalid_segment")
    .when(not_in("status", ["active", "inactive"]), "invalid_status"))

cust_bad = to_quarantine(c.filter(F.col("reason").isNotNull()), "bronze_customer", "customer_id")
silver_customer = dedupe(c.filter(F.col("reason").isNull()).drop("reason", "signup_date_raw"),
                         ["customer_id"],
                         [F.col("updated_at").desc_nulls_last(), F.col("_ingested_at").desc()])
publish(cust_bad, "silver_quarantine_customer")
publish(silver_customer, "silver_customer")

# CELL ********************

# ---------------- products ----------------
p = (spark.table("bronze_product").select(
        F.trim(F.col("product_id")).alias("product_id"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.trim(F.col("category")).alias("category"),
        F.trim(F.col("list_price")).alias("list_price_raw"),
        F.lower(F.trim(F.col("active"))).alias("active"),
        F.trim(F.col("launch_date")).alias("launch_date_raw"),
        F.trim(F.col("updated_at")).alias("updated_at"),
        F.col("_source_file"), F.col("_ingested_at")))
p = (p.withColumn("list_price", F.when(F.col("list_price_raw").rlike(DEC_RE),
                                      F.col("list_price_raw").cast("decimal(18,2)")))
      .withColumn("launch_date", F.to_date(F.col("launch_date_raw"), "yyyy-MM-dd")))
p = p.withColumn("reason", F.when(is_blank("product_id"), "missing_product_id")
    .when(is_blank("product_name"), "missing_product_name")
    .when(not_in("category", ALLOWED_CATEGORIES), "invalid_category")
    .when(F.col("list_price").isNull() | (F.col("list_price") <= 0), "invalid_list_price")
    .when(F.col("launch_date").isNull(), "invalid_launch_date")
    .when(not_in("active", ["true", "false"]), "invalid_active_flag"))

prod_bad = to_quarantine(p.filter(F.col("reason").isNotNull()), "bronze_product", "product_id")
silver_product = dedupe(p.filter(F.col("reason").isNull()).drop("reason", "list_price_raw", "launch_date_raw"),
                        ["product_id"],
                        [F.col("updated_at").desc_nulls_last(), F.col("_ingested_at").desc()])
publish(prod_bad, "silver_quarantine_product")
publish(silver_product, "silver_product")

# CELL ********************

# ---------------- orders: malformed -> quarantine, unknown customer -> rejected ----------------
KEY_CUST = spark.table("silver_customer").select("customer_id").distinct()

o = (spark.table("bronze_order").select(
        F.trim(F.col("order_id")).alias("order_id"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.trim(F.col("order_date")).alias("order_date_raw"),
        F.lower(F.trim(F.col("status"))).alias("status"),
        F.lower(F.trim(F.col("channel"))).alias("channel"),
        F.upper(F.trim(F.col("currency"))).alias("currency"),
        F.lower(F.trim(F.col("source_system"))).alias("source_system"),
        F.col("_source_file"), F.col("_ingested_at")))
o = o.withColumn("order_date", F.to_date(F.col("order_date_raw"), "yyyy-MM-dd"))
o = o.withColumn("reason", F.when(is_blank("order_id"), "missing_order_id")
    .when(is_blank("customer_id"), "missing_customer_id")
    .when(F.col("order_date").isNull(), "invalid_order_date")
    .when(not_in("status", ALLOWED_STATUS), "invalid_status")
    .when(is_blank("channel"), "missing_channel")
    .when(not_in("currency", ALLOWED_CURRENCY), "invalid_currency"))

order_bad = to_quarantine(o.filter(F.col("reason").isNotNull()), "bronze_order", "order_id")
o_good = o.filter(F.col("reason").isNull()).drop("reason")

valid_o = o_good.join(KEY_CUST, "customer_id", "left_semi")
silver_rejected_order = (o_good.join(KEY_CUST, "customer_id", "left_anti")
                         .drop("order_date_raw")
                         .withColumn("reason", F.lit("unknown_customer")))
silver_rejected_order = silver_rejected_order.withColumn("record_key", F.col("order_id")) \
    .withColumn("source_table", F.lit("bronze_order")) \
    .withColumn("quarantined_at", NOW).withColumn("run_id", F.lit(RUN_ID)) \
    .withColumn("source_file", F.col("_source_file")).withColumn("ingested_at", F.col("_ingested_at"))

silver_order = dedupe(valid_o.drop("order_date_raw"), ["order_id"],
                      [F.col("_ingested_at").desc(), F.col("order_date").desc()])

publish(order_bad, "silver_quarantine_order")
publish(silver_rejected_order,
        "silver_rejected_order")
publish(silver_order, "silver_order")

# CELL ********************

# ---------------- order lines: malformed -> quarantine, orphan order/product -> rejected ----------------
KEY_ORDER = spark.table("silver_order").select("order_id").distinct()
KEY_PROD = spark.table("silver_product").select("product_id").distinct()

l = (spark.table("bronze_order_line").select(
        F.trim(F.col("order_line_id")).alias("order_line_id"),
        F.trim(F.col("order_id")).alias("order_id"),
        F.trim(F.col("product_id")).alias("product_id"),
        F.trim(F.col("quantity")).alias("quantity_raw"),
        F.trim(F.col("unit_price")).alias("unit_price_raw"),
        F.trim(F.col("discount_pct")).alias("discount_pct_raw"),
        F.col("_source_file"), F.col("_ingested_at")))
l = (l.withColumn("quantity", F.when(F.col("quantity_raw").rlike(INT_RE),
                                     F.col("quantity_raw").cast("int")))
      .withColumn("unit_price", F.when(F.col("unit_price_raw").rlike(DEC_RE),
                                       F.col("unit_price_raw").cast("decimal(18,2)")))
      .withColumn("discount_pct", F.when(F.col("discount_pct_raw").rlike(INT_RE),
                                         F.col("discount_pct_raw").cast("int"))))
l = l.withColumn("reason", F.when(is_blank("order_line_id"), "missing_order_line_id")
    .when(is_blank("order_id"), "missing_order_id")
    .when(is_blank("product_id"), "missing_product_id")
    .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
    .when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "invalid_unit_price")
    .when(F.col("discount_pct").isNull() | (F.col("discount_pct") < 0) | (F.col("discount_pct") > 100),
          "invalid_discount_pct"))

line_bad = to_quarantine(l.filter(F.col("reason").isNotNull()), "bronze_order_line", "order_line_id")
l_good = dedupe(l.filter(F.col("reason").isNull()).drop("reason", "quantity_raw", "unit_price_raw", "discount_pct_raw"),
                ["order_line_id"],
                [F.col("quantity").desc(), F.col("unit_price").desc(), F.col("_ingested_at").desc()])

orphan_order = (l_good.join(KEY_ORDER, "order_id", "left_anti")
                .withColumn("reason", F.lit("unknown_order_id")))
orphan_prod = (l_good.join(KEY_ORDER, "order_id", "left_semi")
               .join(KEY_PROD, "product_id", "left_anti")
               .withColumn("reason", F.lit("unknown_product_id")))

silver_rejected_order_line = orphan_order.unionByName(orphan_prod, allowMissingColumns=True)
silver_rejected_order_line = (silver_rejected_order_line
                              .withColumn("record_key", F.col("order_line_id"))
                              .withColumn("source_table", F.lit("bronze_order_line"))
                              .withColumn("quarantined_at", NOW)
                              .withColumn("run_id", F.lit(RUN_ID))
                              .withColumn("source_file", F.col("_source_file"))
                              .withColumn("ingested_at", F.col("_ingested_at")))

joined = (l_good.join(KEY_ORDER, "order_id", "left_semi")
                .join(KEY_PROD, "product_id", "left_semi"))
silver_order_line = (joined
    .withColumn("gross_revenue", F.round(F.col("quantity") * F.col("unit_price"), 2).cast("decimal(18,2)"))
    .withColumn("net_revenue", F.round(F.col("quantity") * F.col("unit_price")
                                       * (F.lit(1) - F.col("discount_pct").cast("decimal(9,4)") / F.lit(100)),
                                       2).cast("decimal(18,2)")))

publish(line_bad, "silver_quarantine_order_line")
publish(silver_rejected_order_line, "silver_rejected_order_line")
publish(silver_order_line, "silver_order_line")

# CELL ********************

q = (spark.table("silver_quarantine_customer")
     .unionByName(spark.table("silver_quarantine_product"), allowMissingColumns=True)
     .unionByName(spark.table("silver_quarantine_order"), allowMissingColumns=True)
     .unionByName(spark.table("silver_quarantine_order_line"), allowMissingColumns=True))
publish(q, "silver_quarantine")

quarantine_by_source = {r["source_table"]: r["n"] for r in
                        spark.table("silver_quarantine").groupBy("source_table").count()
                        .withColumnRenamed("count", "n").collect()}
rejected_by_reason = {r["reason"]: r["n"] for r in
                      spark.table("silver_rejected_order").groupBy("reason").count()
                      .withColumnRenamed("count", "n").collect()}
rejected_lines_by_reason = {r["reason"]: r["n"] for r in
                            spark.table("silver_rejected_order_line").groupBy("reason").count()
                            .withColumnRenamed("count", "n").collect()}

mssparkutils.notebook.exit(json.dumps({
    "stage": "silver", "run_id": RUN_ID, "status": "Succeeded",
    "counts": counts,
    "quarantine_by_source": quarantine_by_source,
    "rejected_orders": rejected_by_reason,
    "rejected_order_lines": rejected_lines_by_reason,
}))
