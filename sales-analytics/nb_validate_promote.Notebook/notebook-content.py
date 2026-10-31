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
p_revenue_tolerance = 0.01

# CELL ********************

from pyspark.sql import functions as F
import uuid, json, datetime as dt

spark.conf.set("spark.sql.session.timeZone", "UTC")

RUN_ID = p_run_id or ("validate-" + dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6])
TOL = float(p_revenue_tolerance)

results = []
def check(name, layer, passed, expected, actual, details=""):
    results.append({
        "run_id": RUN_ID, "check_name": name, "layer": layer,
        "status": "PASS" if passed else "FAIL",
        "expected": str(expected), "actual": str(actual), "details": details,
        "checked_at": dt.datetime.utcnow(),
    })
    print(("PASS " if passed else "FAIL ") + name + " | expected=" + str(expected) + " actual=" + str(actual))
    return passed

# CELL ********************

try:
    # Landing files are re-read straight from OneLake, exactly as Bronze does, so the
    # reconciliation is independent of the Bronze/Silver tables it is checking.
    for s in ["customer", "product", "order", "order_line"]:
        (spark.read.option("header", True).option("inferSchema", False)
         .csv("Files/landing/" + s + ".csv")
         .createOrReplaceTempView("src_" + s))

    # Validity rules -- kept identical to the Silver notebook predicates.
    EMAIL_RX = '^[^@ ]+@[^@ ]+[.][^@ ]+$'
    DEC_RX = '^[0-9]+([.][0-9]+)?$'
    INT_RX = '^[0-9]+$'

    PRED = {
        "customer": ("TRIM(customer_id) <> '' AND to_date(signup_date,'yyyy-MM-dd') IS NOT NULL "
                     "AND LOWER(TRIM(email)) RLIKE '" + EMAIL_RX + "' "
                     "AND TRIM(segment) IN ('Consumer','SMB','Enterprise','PublicSector') "
                     "AND LOWER(TRIM(status)) IN ('active','inactive')"),
        "product": ("TRIM(product_id) <> '' AND TRIM(product_name) <> '' "
                    "AND TRIM(category) IN ('Electronics','Home','Outdoors','Apparel','Grocery') "
                    "AND TRIM(list_price) RLIKE '" + DEC_RX + "' AND CAST(TRIM(list_price) AS DECIMAL(18,2)) > 0 "
                    "AND to_date(launch_date,'yyyy-MM-dd') IS NOT NULL "
                    "AND LOWER(TRIM(active)) IN ('true','false')"),
        "order": ("TRIM(order_id) <> '' AND TRIM(customer_id) <> '' "
                  "AND to_date(order_date,'yyyy-MM-dd') IS NOT NULL "
                  "AND LOWER(TRIM(status)) IN ('completed','shipped','pending','cancelled') "
                  "AND TRIM(channel) <> '' "
                  "AND UPPER(TRIM(currency)) IN ('USD','EUR','GBP','CAD','AUD','JPY','BRL')"),
        "order_line": ("TRIM(order_line_id) <> '' AND TRIM(order_id) <> '' AND TRIM(product_id) <> '' "
                       "AND TRIM(quantity) RLIKE '" + INT_RX + "' AND CAST(quantity AS INT) > 0 "
                       "AND TRIM(unit_price) RLIKE '" + DEC_RX + "' AND CAST(unit_price AS DECIMAL(18,2)) > 0 "
                       "AND TRIM(discount_pct) RLIKE '" + INT_RX + "' "
                       "AND CAST(discount_pct AS INT) BETWEEN 0 AND 100"),
    }
    KEYS = {"customer": "customer_id", "product": "product_id",
            "order": "order_id", "order_line": "order_line_id"}


    def sql_int(q):
        return int(spark.sql(q).collect()[0][0])
except Exception:
    import traceback, urllib.request
    _tb = traceback.format_exc()
    try:
        _tok = mssparkutils.credentials.getToken('storage')
        _u = ('https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/Files/../').replace('/Files/../', '/')
    except Exception:
        _tok = None
    try:
        _url = 'https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/cd7d666e-33c5-4bed-8fe7-8512e9bbaa34/Files/_ops/validate_error.txt'
        _steps = [('PUT', _url + '?resource=file', b'', {'Content-Length': '0'}),
                  ('PATCH', _url + '?action=append&position=0', _tb.encode(), {}),
                  ('PATCH', _url + '?action=flush&position=' + str(len(_tb.encode())), b'', {'Content-Length': '0'})]
        for _m, _uu, _d, _hh in _steps:
            _r = urllib.request.Request(_uu, data=_d, method=_m)
            _r.add_header('Authorization', 'Bearer ' + str(_tok))
            for _k, _v in _hh.items():
                _r.add_header(_k, _v)
            urllib.request.urlopen(_r, timeout=30).read()
    except Exception as _e:
        print('could not persist traceback:', _e)
    raise

# CELL ********************

try:
    # ---- 1. Bronze fidelity: bronze rows must equal physical lines in the landing file
    WANTED = {"bronze_customer": "customer", "bronze_product": "product",
              "bronze_order": "order", "bronze_order_line": "order_line"}
    for table, src in WANTED.items():
        lines = spark.read.text("Files/landing/" + src + ".csv").count() - 1
        rows = spark.table(table).count()
        check("bronze_row_count:" + table, "bronze", rows == lines, lines, rows, "source file " + src + ".csv")

    # ---- 2. Bronze -> Silver conservation:
    #      bronze = accepted + quarantined + rejected + duplicates collapsed on the business key
    q = spark.table("silver_quarantine")
    q_counts = {r["source_table"]: int(r["n"]) for r in
                q.groupBy("source_table").count().withColumnRenamed("count", "n").collect()}
    rej_order = spark.table("silver_rejected_order").count()
    rej_line = spark.table("silver_rejected_order_line").count()

    collapsed = {}
    for src in ["customer", "product", "order", "order_line"]:
        k = KEYS[src]
        collapsed[src] = sql_int("SELECT COUNT(*) - COUNT(DISTINCT TRIM(" + k + ")) FROM src_" + src
                                 + " WHERE " + PRED[src])

    accepted = {"customer": spark.table("silver_customer").count(),
                "product": spark.table("silver_product").count(),
                "order": spark.table("silver_order").count(),
                "order_line": spark.table("silver_order_line").count()}
    rejected = {"customer": 0, "product": 0, "order": rej_order, "order_line": rej_line}

    for src in ["customer", "product", "order", "order_line"]:
        bronze_n = sql_int("SELECT COUNT(*) FROM src_" + src)
        quarantined = q_counts.get("bronze_" + src, 0)
        accounted = accepted[src] + quarantined + rejected[src] + collapsed[src]
        check("bronze_to_silver_conservation:" + src, "silver", bronze_n == accounted, bronze_n, accounted,
              "accepted=" + str(accepted[src]) + " quarantine=" + str(quarantined)
              + " rejected=" + str(rejected[src]) + " deduped=" + str(collapsed[src]))
except Exception:
    import traceback, urllib.request
    _tb = traceback.format_exc()
    try:
        _tok = mssparkutils.credentials.getToken('storage')
        _u = ('https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/Files/../').replace('/Files/../', '/')
    except Exception:
        _tok = None
    try:
        _url = 'https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/cd7d666e-33c5-4bed-8fe7-8512e9bbaa34/Files/_ops/validate_error.txt'
        _steps = [('PUT', _url + '?resource=file', b'', {'Content-Length': '0'}),
                  ('PATCH', _url + '?action=append&position=0', _tb.encode(), {}),
                  ('PATCH', _url + '?action=flush&position=' + str(len(_tb.encode())), b'', {'Content-Length': '0'})]
        for _m, _uu, _d, _hh in _steps:
            _r = urllib.request.Request(_uu, data=_d, method=_m)
            _r.add_header('Authorization', 'Bearer ' + str(_tok))
            for _k, _v in _hh.items():
                _r.add_header(_k, _v)
            urllib.request.urlopen(_r, timeout=30).read()
    except Exception as _e:
        print('could not persist traceback:', _e)
    raise

# CELL ********************

try:
    # ---- 3. Business-key uniqueness in silver
    for table, key in [("silver_customer", "customer_id"), ("silver_product", "product_id"),
                       ("silver_order", "order_id"), ("silver_order_line", "order_line_id")]:
        dup = spark.table(table).groupBy(key).count().filter(F.col("count") > 1).count()
        check("silver_unique:" + table, "silver", dup == 0, 0, dup, key)

    # ---- 4. Referential integrity: no orphan foreign keys may survive in silver
    orphan_orders = (spark.table("silver_order").alias("o")
                     .join(spark.table("silver_customer").select("customer_id").alias("c"),
                           F.col("o.customer_id") == F.col("c.customer_id"), "left_anti").count())
    orphan_lines_order = (spark.table("silver_order_line").alias("l")
                          .join(spark.table("silver_order").select("order_id").alias("o"),
                                F.col("l.order_id") == F.col("o.order_id"), "left_anti").count())
    orphan_lines_product = (spark.table("silver_order_line").alias("l")
                            .join(spark.table("silver_product").select("product_id").alias("p"),
                                  F.col("l.product_id") == F.col("p.product_id"), "left_anti").count())
    check("referential_integrity:order.customer_id", "silver", orphan_orders == 0, 0, orphan_orders)
    check("referential_integrity:order_line.order_id", "silver", orphan_lines_order == 0, 0, orphan_lines_order)
    check("referential_integrity:order_line.product_id", "silver", orphan_lines_product == 0, 0, orphan_lines_product)

    # ---- 5. No null or empty identifiers in silver
    null_cust = spark.table("silver_order").filter(F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == "")).count()
    null_prod = spark.table("silver_order_line").filter(F.col("product_id").isNull() | (F.trim(F.col("product_id")) == "")).count()
    null_ordl = spark.table("silver_order_line").filter(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == "")).count()
    check("null_identifiers:silver_order.customer_id", "silver", null_cust == 0, 0, null_cust)
    check("null_identifiers:silver_order_line.product_id", "silver", null_prod == 0, 0, null_prod)
    check("null_identifiers:silver_order_line.order_id", "silver", null_ordl == 0, 0, null_ordl)
except Exception:
    import traceback, urllib.request
    _tb = traceback.format_exc()
    try:
        _tok = mssparkutils.credentials.getToken('storage')
        _u = ('https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/Files/../').replace('/Files/../', '/')
    except Exception:
        _tok = None
    try:
        _url = 'https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/cd7d666e-33c5-4bed-8fe7-8512e9bbaa34/Files/_ops/validate_error.txt'
        _steps = [('PUT', _url + '?resource=file', b'', {'Content-Length': '0'}),
                  ('PATCH', _url + '?action=append&position=0', _tb.encode(), {}),
                  ('PATCH', _url + '?action=flush&position=' + str(len(_tb.encode())), b'', {'Content-Length': '0'})]
        for _m, _uu, _d, _hh in _steps:
            _r = urllib.request.Request(_uu, data=_d, method=_m)
            _r.add_header('Authorization', 'Bearer ' + str(_tok))
            for _k, _v in _hh.items():
                _r.add_header(_k, _v)
            urllib.request.urlopen(_r, timeout=30).read()
    except Exception as _e:
        print('could not persist traceback:', _e)
    raise

# CELL ********************

try:
    # ---- 6. Rejected / quarantined records exist and match the recorded breakdown
    qn = q.count()
    check("rejected_records_present", "silver", (qn + rej_order + rej_line) > 0, "> 0", qn + rej_order + rej_line,
          "quarantine=" + str(qn) + " rejected_orders=" + str(rej_order) + " rejected_lines=" + str(rej_line))
    check("rejected_orders_unknown_customer", "silver", rej_order > 0, "> 0", rej_order)
    check("quarantined_malformed", "silver", qn > 0, "> 0", qn)
    for table, n in q_counts.items():
        check("quarantine_breakdown:" + table, "silver", n > 0, "> 0", n)
except Exception:
    import traceback, urllib.request
    _tb = traceback.format_exc()
    try:
        _tok = mssparkutils.credentials.getToken('storage')
        _u = ('https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/Files/../').replace('/Files/../', '/')
    except Exception:
        _tok = None
    try:
        _url = 'https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/cd7d666e-33c5-4bed-8fe7-8512e9bbaa34/Files/_ops/validate_error.txt'
        _steps = [('PUT', _url + '?resource=file', b'', {'Content-Length': '0'}),
                  ('PATCH', _url + '?action=append&position=0', _tb.encode(), {}),
                  ('PATCH', _url + '?action=flush&position=' + str(len(_tb.encode())), b'', {'Content-Length': '0'})]
        for _m, _uu, _d, _hh in _steps:
            _r = urllib.request.Request(_uu, data=_d, method=_m)
            _r.add_header('Authorization', 'Bearer ' + str(_tok))
            for _k, _v in _hh.items():
                _r.add_header(_k, _v)
            urllib.request.urlopen(_r, timeout=30).read()
    except Exception as _e:
        print('could not persist traceback:', _e)
    raise

# CELL ********************

try:
    # ---- 7. Source-to-Gold revenue reconciliation
    src = spark.sql("""
    WITH cust AS (
      SELECT TRIM(customer_id) AS customer_id FROM src_customer
      WHERE """ + PRED["customer"] + """
    ), prod AS (
      SELECT TRIM(product_id) AS product_id FROM src_product
      WHERE """ + PRED["product"] + """
    ), ord AS (
      SELECT DISTINCT TRIM(order_id) AS order_id, TRIM(customer_id) AS customer_id FROM src_order
      WHERE """ + PRED["order"] + """
    ), ord_valid AS (
      SELECT o.order_id FROM ord o LEFT SEMI JOIN cust c ON o.customer_id = c.customer_id
    ), lin AS (
      SELECT TRIM(order_line_id) AS order_line_id, TRIM(order_id) AS order_id, TRIM(product_id) AS product_id,
             quantity, unit_price, discount_pct,
             ROW_NUMBER() OVER (PARTITION BY TRIM(order_line_id)
                                ORDER BY CAST(quantity AS INT) DESC,
                                         CAST(unit_price AS DECIMAL(18,2)) DESC) AS rn
      FROM src_order_line
      WHERE """ + PRED["order_line"] + """
    )
    SELECT COUNT(*) AS valid_lines,
           CAST(SUM(ROUND(CAST(quantity AS DECIMAL(9,0)) * CAST(unit_price AS DECIMAL(18,2))
                          * (1 - CAST(discount_pct AS DECIMAL(9,4)) / 100), 2)) AS DECIMAL(18,2)) AS net_revenue
    FROM lin
    LEFT SEMI JOIN ord_valid ov ON lin.order_id = ov.order_id
    LEFT SEMI JOIN prod pr ON lin.product_id = pr.product_id
    WHERE lin.rn = 1
    """).collect()[0]

    src_lines = int(src["valid_lines"])
    src_net = float(src["net_revenue"] or 0)

    silver_agg = spark.sql("SELECT COUNT(*) AS n, ROUND(SUM(net_revenue),2) AS rev FROM silver_order_line").collect()[0]
    silver_lines = int(silver_agg["n"])
    silver_net = float(silver_agg["rev"])


    def gold_sum(t):
        return float(spark.sql("SELECT ROUND(SUM(net_revenue),2) AS rev FROM " + t).collect()[0]["rev"] or 0)


    gold_net = gold_sum("gold_stg_daily_sales")
    gold_cust_net = gold_sum("gold_stg_customer_sales")
    gold_prod_net = gold_sum("gold_stg_product_performance")
    gold_month_net = gold_sum("gold_stg_monthly_revenue_by_segment")

    check("source_to_silver_line_count", "reconciliation", src_lines == silver_lines, src_lines, silver_lines)
    check("source_to_silver_revenue", "reconciliation", abs(src_net - silver_net) <= TOL, round(src_net, 2), round(silver_net, 2))
    check("source_to_gold_revenue", "reconciliation", abs(src_net - gold_net) <= TOL, round(src_net, 2), round(gold_net, 2),
          "tolerance=" + str(TOL))
    check("gold_daily_matches_customer_sales", "reconciliation", abs(gold_net - gold_cust_net) <= TOL, round(gold_net, 2), round(gold_cust_net, 2))
    check("gold_daily_matches_product_perf", "reconciliation", abs(gold_net - gold_prod_net) <= TOL, round(gold_net, 2), round(gold_prod_net, 2))
    check("gold_daily_matches_monthly_segment", "reconciliation", abs(gold_net - gold_month_net) <= TOL, round(gold_net, 2), round(gold_month_net, 2))
except Exception:
    import traceback, urllib.request
    _tb = traceback.format_exc()
    try:
        _tok = mssparkutils.credentials.getToken('storage')
        _u = ('https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/Files/../').replace('/Files/../', '/')
    except Exception:
        _tok = None
    try:
        _url = 'https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/cd7d666e-33c5-4bed-8fe7-8512e9bbaa34/Files/_ops/validate_error.txt'
        _steps = [('PUT', _url + '?resource=file', b'', {'Content-Length': '0'}),
                  ('PATCH', _url + '?action=append&position=0', _tb.encode(), {}),
                  ('PATCH', _url + '?action=flush&position=' + str(len(_tb.encode())), b'', {'Content-Length': '0'})]
        for _m, _uu, _d, _hh in _steps:
            _r = urllib.request.Request(_uu, data=_d, method=_m)
            _r.add_header('Authorization', 'Bearer ' + str(_tok))
            for _k, _v in _hh.items():
                _r.add_header(_k, _v)
            urllib.request.urlopen(_r, timeout=30).read()
    except Exception as _e:
        print('could not persist traceback:', _e)
    raise

# CELL ********************

try:
    # ---- 8. Gold staging sanity
    for t, key in [("gold_stg_daily_sales", "order_date"),
                   ("gold_stg_customer_sales", "customer_id"),
                   ("gold_stg_product_performance", "product_id"),
                   ("gold_stg_monthly_revenue_by_segment", "revenue_month")]:
        df = spark.table(t)
        n = df.count()
        nulls = df.filter(F.col(key).isNull()).count()
        neg = df.filter(F.col("net_revenue") < 0).count()
        dup = df.groupBy(key).count().filter(F.col("count") > 1).count()
        check("gold_staging_rows:" + t, "gold", n > 0, "> 0", n)
        check("gold_staging_null_key:" + t, "gold", nulls == 0, 0, nulls, key)
        check("gold_staging_negative_revenue:" + t, "gold", neg == 0, 0, neg)
        if t != "gold_stg_monthly_revenue_by_segment":
            check("gold_staging_grain_unique:" + t, "gold", dup == 0, 0, dup, key)
except Exception:
    import traceback, urllib.request
    _tb = traceback.format_exc()
    try:
        _tok = mssparkutils.credentials.getToken('storage')
        _u = ('https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/Files/../').replace('/Files/../', '/')
    except Exception:
        _tok = None
    try:
        _url = 'https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/cd7d666e-33c5-4bed-8fe7-8512e9bbaa34/Files/_ops/validate_error.txt'
        _steps = [('PUT', _url + '?resource=file', b'', {'Content-Length': '0'}),
                  ('PATCH', _url + '?action=append&position=0', _tb.encode(), {}),
                  ('PATCH', _url + '?action=flush&position=' + str(len(_tb.encode())), b'', {'Content-Length': '0'})]
        for _m, _uu, _d, _hh in _steps:
            _r = urllib.request.Request(_uu, data=_d, method=_m)
            _r.add_header('Authorization', 'Bearer ' + str(_tok))
            for _k, _v in _hh.items():
                _r.add_header(_k, _v)
            urllib.request.urlopen(_r, timeout=30).read()
    except Exception as _e:
        print('could not persist traceback:', _e)
    raise

# CELL ********************

try:
    # ---- 9. Persist results and gate promotion
    RESULT_SCHEMA = ("run_id string, check_name string, layer string, status string, "
                     "expected string, actual string, details string, checked_at timestamp")
    (spark.createDataFrame(results, RESULT_SCHEMA)
          .write.mode("append").option("mergeSchema", "true")
          .format("delta").saveAsTable("ops_validation_results"))

    failed = [r["check_name"] for r in results if r["status"] == "FAIL"]
    promotions = [("gold_daily_sales", "gold_stg_daily_sales"),
                  ("gold_customer_sales", "gold_stg_customer_sales"),
                  ("gold_product_performance", "gold_stg_product_performance"),
                  ("gold_monthly_revenue_by_segment", "gold_stg_monthly_revenue_by_segment")]

    if failed:
        gate = {"promotion": "BLOCKED", "failed_checks": failed}
        (spark.createDataFrame([(RUN_ID, "gold_promotion_gate", "gold", "FAIL", "all checks pass",
                                 "BLOCKED", ",".join(failed), dt.datetime.utcnow())], RESULT_SCHEMA)
              .write.mode("append").option("mergeSchema", "true")
              .format("delta").saveAsTable("ops_validation_results"))
        print(json.dumps(gate, indent=2))
        raise Exception("Gold promotion blocked: " + ", ".join(failed))

    for target, staging in promotions:
        spark.sql("CREATE OR REPLACE TABLE " + target + " AS SELECT * FROM " + staging)
    promoted = {t: spark.table(t).count() for t, _ in promotions}
    (spark.createDataFrame([(RUN_ID, "gold_promotion_gate", "gold", "PASS", "all checks pass",
                             "PROMOTED", json.dumps(promoted), dt.datetime.utcnow())], RESULT_SCHEMA)
          .write.mode("append").option("mergeSchema", "true")
          .format("delta").saveAsTable("ops_validation_results"))

    mssparkutils.notebook.exit(json.dumps({
        "stage": "validate_promote", "run_id": RUN_ID, "checks": len(results), "failed": failed,
        "source_lines": src_lines, "source_net_revenue": round(src_net, 2),
        "silver_lines": silver_lines, "silver_net_revenue": round(silver_net, 2),
        "gold_net_revenue": round(gold_net, 2),
        "deduped_collapsed": collapsed,
        "quarantine": q_counts, "rejected_orders": rej_order, "rejected_order_lines": rej_line,
        "promoted": promoted,
    }))
except Exception:
    import traceback, urllib.request
    _tb = traceback.format_exc()
    try:
        _tok = mssparkutils.credentials.getToken('storage')
        _u = ('https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/Files/../').replace('/Files/../', '/')
    except Exception:
        _tok = None
    try:
        _url = 'https://onelake.dfs.fabric.microsoft.com/44eaebf6-c640-4af4-a272-8dd8d577247b/cd7d666e-33c5-4bed-8fe7-8512e9bbaa34/Files/_ops/validate_error.txt'
        _steps = [('PUT', _url + '?resource=file', b'', {'Content-Length': '0'}),
                  ('PATCH', _url + '?action=append&position=0', _tb.encode(), {}),
                  ('PATCH', _url + '?action=flush&position=' + str(len(_tb.encode())), b'', {'Content-Length': '0'})]
        for _m, _uu, _d, _hh in _steps:
            _r = urllib.request.Request(_uu, data=_d, method=_m)
            _r.add_header('Authorization', 'Bearer ' + str(_tok))
            for _k, _v in _hh.items():
                _r.add_header(_k, _v)
            urllib.request.urlopen(_r, timeout=30).read()
    except Exception as _e:
        print('could not persist traceback:', _e)
    raise
