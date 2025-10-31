#!/usr/bin/env python
# Convert LOCAL CUR month -> fixtures/athena_cost.csv
# Optional: synthesize elbv2/elb/metrics fixtures (zero-traffic) for fully offline runs.
import argparse, os, glob, json, re, duckdb, shutil, csv

def detect_format(cur_dir):
    pq  = glob.glob(os.path.join(cur_dir, '**', '*.parquet'), recursive=True)
    gz  = glob.glob(os.path.join(cur_dir, '**', '*.csv.gz'), recursive=True)
    csvs = glob.glob(os.path.join(cur_dir, '**', '*.csv'), recursive=True)
    if pq:  return 'parquet'
    if gz:  return 'csv.gz'
    if csvs: return 'csv'
    raise SystemExit("No Parquet/CSV files found under cur_dir")

def arn_parts(arn):
    m = re.match(r"arn:aws:elasticloadbalancing:([a-z0-9-]+):([0-9]{12}):loadbalancer/(.+)", arn or '')
    if not m: return None
    region, acct, rest = m.groups()
    kind = rest.split('/')[0] if '/' in rest else 'classic'
    short = rest
    return {'region': region, 'account': acct, 'kind': kind, 'short': short}

def pick(cols, candidates, required=False, default=None):
    for c in candidates:
        if c in cols: return c
        for cc in cols:
            if cc.lower() == c.lower():
                return cc
    if required:
        raise SystemExit(f"Missing required column. Tried: {candidates}, found: {sorted(cols)[:50]} ...")
    return default

def synthesize_lb_fixtures(rows_csv_path, out_dir):
    elbv2 = {"Regions": []}
    elbcl = {"Regions": []}
    metrics = {}
    by_region_v2 = {}
    by_region_cl = {}

    with open(rows_csv_path, newline='') as f:
        for r in csv.DictReader(f):
            arn = r['Resource Id']
            p = arn_parts(arn)
            if not p:
                continue
            if p['kind'] in ('app','net','gateway'):
                lst = by_region_v2.setdefault(p['region'], [])
                lb_type = {'app':'application','net':'network','gateway':'gateway'}.get(p['kind'],'application')
                name = p['short'].split('/')[1] if '/' in p['short'] else p['short']
                lst.append({
                    "LoadBalancerArn": arn,
                    "LoadBalancerName": name,
                    "Scheme": "internet-facing",
                    "Type": lb_type,
                    "VpcId": "vpc-unknown",
                    "AvailabilityZones": [],
                    "CreatedTime": "2024-01-01T00:00:00+00:00",
                    "SecurityGroups": []
                })
                metrics[p['short']] = {
                    "RequestCountSum": 0,
                    "ProcessedBytesSum": 0,
                    "ActiveFlowCountAverage": 0.0
                }
            else:
                lst = by_region_cl.setdefault(p['region'], [])
                name = p['short'].split('/')[-1]
                lst.append({
                    "LoadBalancerName": name,
                    "CreatedTime": "2024-01-01T00:00:00+00:00",
                    "AvailabilityZones": [],
                    "VPCId": "vpc-unknown",
                    "SecurityGroups": []
                })
                metrics[name] = {"RequestCountSum": 0}

    for reg, lbs in by_region_v2.items():
        elbv2["Regions"].append({"Region": reg, "LoadBalancers": lbs})
    for reg, lbs in by_region_cl.items():
        elbcl["Regions"].append({"Region": reg, "LoadBalancerDescriptions": lbs})

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, 'elbv2_describe.json'), 'w') as f:
        json.dump(elbv2, f, indent=2)
    with open(os.path.join(out_dir, 'elb_describe.json'), 'w') as f:
        json.dump(elbcl, f, indent=2)
    with open(os.path.join(out_dir, 'metrics.json'), 'w') as f:
        json.dump(metrics, f, indent=2)

def main():
    ap = argparse.ArgumentParser(description="Convert local CUR month to fixtures.")
    ap.add_argument('--cur-dir', required=True, help='Dir with ONE BILLING_PERIOD (e.g., cur_raw/BILLING_PERIOD=2025-10)')
    ap.add_argument('--fixtures-dir', default='fixtures', help='Output dir for fixtures')
    ap.add_argument('--synthesize-lb-fixtures', action='store_true', help='Also write elbv2/elb/metrics fixtures')
    ap.add_argument('--mem-gb', type=int, default=2, help='DuckDB memory limit in GB (spills to disk)')
    ap.add_argument('--threads', type=int, default=2, help='DuckDB threads')
    args = ap.parse_args()

    fmt = detect_format(args.cur_dir)

    # Spill to disk / tame memory
    spill_dir = os.path.join('.duckdb_tmp')
    if os.path.exists(spill_dir):
        shutil.rmtree(spill_dir)
    os.makedirs(spill_dir, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"PRAGMA memory_limit='{args.mem_gb}GB';")
    con.execute(f"PRAGMA threads={args.threads};")
    con.execute("PRAGMA preserve_insertion_order=false;")
    con.execute(f"PRAGMA temp_directory='{spill_dir}';")
    try:
        con.execute("PRAGMA enable_progress_bar=true;")
    except Exception:
        pass

    # Materialize a TEMP VIEW for the whole month (glob)
    if fmt == 'parquet':
        src_sql = f"SELECT * FROM read_parquet('{args.cur_dir}/**/*.parquet')"
    elif fmt == 'csv.gz':
        src_sql = f"SELECT * FROM read_csv_auto('{args.cur_dir}/**/*.csv.gz', HIVE_PARTITIONING=TRUE)"
    else:
        src_sql = f"SELECT * FROM read_csv_auto('{args.cur_dir}/**/*.csv', HIVE_PARTITIONING=TRUE)"

    con.execute("DROP VIEW IF EXISTS cur_src;")
    con.execute(f"CREATE TEMP VIEW cur_src AS {src_sql};")

    # Introspect available columns
    cols = {r[1] for r in con.execute("PRAGMA table_info('cur_src')").fetchall()}

    # Map column names present in your CUR
    product_code_col   = pick(cols, ['line_item_product_code','productcode','product_servicecode','product_service_code'], required=True)
    resource_id_col    = pick(cols, ['line_item_resource_id','resourceid','resource_id'], required=True)
    usage_acct_col     = pick(cols, ['line_item_usage_account_id','usageaccountid','usage_account_id','bill_payer_account_id','payer_account_id'], required=True)
    region_col         = pick(cols, ['product_region','product_region_code','aws_region','region'], required=False, default=None)
    unblended_cost_col = pick(cols, ['line_item_unblended_cost','unblendedcost','unblended_cost','cost'], required=True)
    usage_amount_col   = pick(cols, ['line_item_usage_amount','usageamount','usage_amount'], required=True)
    line_item_type_col = pick(cols, ['line_item_line_item_type','line_item_type','lineitemtype'], required=False, default=None)

    select_cols = [
        f"{usage_acct_col}   AS usage_account_id",
        (f"{region_col}     AS product_region" if region_col else "'' AS product_region"),
        f"{resource_id_col} AS resource_id",
        f"CAST({usage_amount_col} AS DOUBLE)   AS usage_qty",
        f"CAST({unblended_cost_col} AS DOUBLE) AS unblended_cost",
    ]
    where_clauses = [f"{product_code_col} = 'AWSELB'", f"{resource_id_col} IS NOT NULL"]
    if line_item_type_col:
        where_clauses.append(f"({line_item_type_col} IN ('Usage','DiscountedUsage') OR {line_item_type_col} IS NULL)")

    sql_stmt = f"""
WITH cur AS (
  SELECT
    {', '.join(select_cols)}
  FROM cur_src
  WHERE {' AND '.join(where_clauses)}
)
SELECT
  usage_account_id   AS "Account Id",
  product_region     AS "Region",
  resource_id        AS "Resource Id",
  SUM(usage_qty)     AS "Monthly Usage Hours",
  ROUND(SUM(unblended_cost), 2)            AS "Monthly Cost",
  CAST(ROUND(SUM(unblended_cost) * 12) AS BIGINT) AS "Annual Saving"
FROM cur
GROUP BY usage_account_id, product_region, resource_id
ORDER BY 5 DESC
"""

    os.makedirs(args.fixtures_dir, exist_ok=True)
    out_csv = os.path.join(args.fixtures_dir, 'athena_cost.csv')

    con.execute(f"COPY ({sql_stmt}) TO '{out_csv}' WITH (HEADER, DELIMITER ',');")
    print(f"Wrote cost fixture → {out_csv}")

    if args.synthesize_lb_fixtures:
        synthesize_lb_fixtures(out_csv, args.fixtures_dir)
        print(f"Synthesized LB + metrics fixtures in {args.fixtures_dir}/")

if __name__ == '__main__':
    main()
