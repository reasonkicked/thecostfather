#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Identify Idle Load Balancers (ALB/NLB/Classic) with optional offline fixtures.
- Idle = no traffic during lookback window:
    * ALB: RequestCount Sum == 0 (zero-ish threshold: <=1)
    * NLB: ProcessedBytes Sum == 0 AND ActiveFlowCount Avg == 0 (<=1KB, <=0.01)
    * Classic ELB: RequestCount Sum == 0 (<=1)
- Savings = last full month's unblended cost x 12 from CUR (Athena)

Usage (offline with fixtures):
  python tools/identify_idle_elb.py \
    --offline --fixtures-dir fixtures \
    --lookback-hours 336 --cost-threshold 0 \
    --athena-db wivdb --athena-table wiv_cur --workgroup primary \
    --output out/idle_elb.csv

Later (online with real AWS & CUR):
  export AWS_PROFILE=890769921003_AdministratorAccess
  python tools/identify_idle_elb.py \
    --lookback-hours 336 --cost-threshold 0 \
    --athena-db <db> --athena-table <table> --workgroup <wg> \
    --output out/idle_elb.csv
"""
import argparse
import csv
import datetime
import itertools
import os
import sys
import time
import json

import boto3
from botocore.config import Config

# ---------- Config ----------
CFG = Config(retries={'max_attempts': 10, 'mode': 'standard'})

# ---------- Helpers (online + OFFLINE) ----------

def all_regions(session, offline=False, fixtures_dir='fixtures'):
    if offline:
        # infer regions from fixtures
        f1 = os.path.join(fixtures_dir, 'elbv2_describe.json')
        f2 = os.path.join(fixtures_dir, 'elb_describe.json')
        regs = set()
        if os.path.exists(f1):
            try:
                data = json.load(open(f1))
                for item in data.get('Regions', []):
                    regs.add(item['Region'])
            except Exception:
                pass
        if os.path.exists(f2):
            try:
                data = json.load(open(f2))
                for item in data.get('Regions', []):
                    regs.add(item['Region'])
            except Exception:
                pass
        return sorted(regs or ['us-east-1'])
    # online
    ec2 = session.client('ec2', config=CFG)
    return [r['RegionName'] for r in ec2.describe_regions(AllRegions=True)['Regions']
            if r.get('OptInStatus') in (None, 'opt-in-not-required', 'opted-in')]

def athena_query(session, q, db, workgroup, offline=False, fixtures_dir='fixtures'):
    if offline:
        # fixtures/athena_cost.csv
        path = os.path.join(fixtures_dir, 'athena_cost.csv')
        rows = []
        with open(path, newline='') as f:
            for r in csv.DictReader(f):
                rows.append(r)
        return rows
    # online
    athena = session.client('athena', config=CFG)
    qid = athena.start_query_execution(
        QueryString=q,
        QueryExecutionContext={'Database': db},
        WorkGroup=workgroup
    )['QueryExecutionId']
    while True:
        s = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
        if s in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(1.5)
    if s != 'SUCCEEDED':
        raise RuntimeError(f"Athena query failed: {s}")
    res = athena.get_query_results(QueryExecutionId=qid)
    headers = [c['VarCharValue'] for c in res['ResultSet']['Rows'][0]['Data']]
    rows = []
    for r in res['ResultSet']['Rows'][1:]:
        row = {}
        for h, d in itertools.zip_longest(headers, r['Data']):
            row[h] = d.get('VarCharValue') if d else None
        rows.append(row)
    return rows

def list_lbs(session, region, offline=False, fixtures_dir='fixtures'):
    if offline:
        v2, classic = [], []
        dv2_path = os.path.join(fixtures_dir, 'elbv2_describe.json')
        dcl_path = os.path.join(fixtures_dir, 'elb_describe.json')
        if os.path.exists(dv2_path):
            data_v2 = json.load(open(dv2_path))
            for reg in data_v2.get('Regions', []):
                if reg['Region'] != region:
                    continue
                v2.extend(reg.get('LoadBalancers', []))
        if os.path.exists(dcl_path):
            data_cl = json.load(open(dcl_path))
            for reg in data_cl.get('Regions', []):
                if reg['Region'] != region:
                    continue
                classic.extend(reg.get('LoadBalancerDescriptions', []))
        return v2, classic
    # online
    elbv2 = session.client('elbv2', region_name=region, config=CFG)
    elb = session.client('elb', region_name=region, config=CFG)
    v2 = []
    paginator = elbv2.get_paginator('describe_load_balancers')
    for page in paginator.paginate():
        v2.extend(page.get('LoadBalancers', []))
    classic = []
    paginator = elb.get_paginator('describe_load_balancers')
    for page in paginator.paginate():
        classic.extend(page.get('LoadBalancerDescriptions', []))
    return v2, classic

def metrics_fixture_lookup(fixtures_dir):
    path = os.path.join(fixtures_dir, 'metrics.json')
    return json.load(open(path)) if os.path.exists(path) else {}

def cw_sum(session, region, ns, metric, dims, start, end, stat='Sum',
           offline=False, fixtures=None):
    if offline:
        # For fixtures, dimension key is short name (v2) or CLB name
        key = dims[0]['Value']
        return float(fixtures.get(key, {}).get(metric + stat, 0.0))
    cw = session.client('cloudwatch', region_name=region, config=CFG)
    r = cw.get_metric_statistics(
        Namespace=ns, MetricName=metric, Dimensions=dims,
        StartTime=start, EndTime=end,
        Period=max(300, int((end - start).total_seconds() / 144)),
        Statistics=[stat]
    )
    pts = r.get('Datapoints', [])
    if not pts:
        return 0.0
    return sum(p[stat] for p in pts)

def cw_avg(session, region, ns, metric, dims, start, end,
           offline=False, fixtures=None):
    if offline:
        key = dims[0]['Value']
        return float(fixtures.get(key, {}).get(metric + 'Average', 0.0))
    cw = session.client('cloudwatch', region_name=region, config=CFG)
    r = cw.get_metric_statistics(
        Namespace=ns, MetricName=metric, Dimensions=dims,
        StartTime=start, EndTime=end,
        Period=max(300, int((end - start).total_seconds() / 144)),
        Statistics=['Average']
    )
    pts = r.get('Datapoints', [])
    if not pts:
        return 0.0
    return sum(p['Average'] for p in pts) / len(pts)

def arn_for_classic(lb_name, region, account_id):
    return f"arn:aws:elasticloadbalancing:{region}:{account_id}:loadbalancer/{lb_name}"

def short_lb_id_from_arn(arn):
    # arn:...:loadbalancer/<short>
    if ':loadbalancer/' in arn:
        return arn.split(':loadbalancer/')[1]
    return arn

def to_iso(val):
    """Normalize datetime/string -> ISO8601 string."""
    if not val:
        return ''
    if isinstance(val, str):
        # assume already iso-ish
        return val
    try:
        return val.isoformat()
    except Exception:
        return str(val)

def azs_to_str(v2_az_list):
    if not v2_az_list:
        return ''
    parts = []
    for az in v2_az_list:
        if isinstance(az, dict):
            parts.append(az.get('SubnetId') or az.get('ZoneName') or az.get('ZoneId') or '')
        else:
            parts.append(str(az))
    return ','.join([p for p in parts if p])

# ---------- Main logic ----------

def main():
    ap = argparse.ArgumentParser(description="Identify idle ELBs (ALB/NLB/Classic)")
    ap.add_argument('--lookback-hours', type=int, default=336, help='Hours to analyze for zero traffic')
    ap.add_argument('--cost-threshold', type=float, default=0.0, help='Min annual saving to include ($)')
    ap.add_argument('--athena-db', default='wivdb')
    ap.add_argument('--athena-table', default='wiv_cur')
    ap.add_argument('--workgroup', default='primary')
    ap.add_argument('--regions', nargs='*', help='Subset of regions (default: all available)')
    ap.add_argument('--account-id', default=None, help='Override account id (default: STS caller)')
    ap.add_argument('--output', default='out/idle_elb.csv')
    ap.add_argument('--offline', action='store_true', help='Read from fixtures instead of AWS')
    ap.add_argument('--fixtures-dir', default='fixtures')
    args = ap.parse_args()

    session = boto3.Session()  # respects AWS_PROFILE if set
    regions = args.regions or all_regions(session, offline=args.offline, fixtures_dir=args.fixtures_dir)

    look_end = datetime.datetime.utcnow()
    look_start = look_end - datetime.timedelta(hours=args.lookback_hours)

    # Load SQL and pull CUR cost rows (or fixtures)
    sql_path = os.path.join('sql', 'elb_cost_last_month.sql')
    if not os.path.exists(sql_path):
        sys.exit(f"Missing SQL file: {sql_path}")
    sql = open(sql_path, 'r').read() \
        .replace('${athena_db}', args.athena_db) \
        .replace('${athena_table}', args.athena_table)

    cost_rows = athena_query(session, sql, args.athena_db, args.workgroup,
                             offline=args.offline, fixtures_dir=args.fixtures_dir)
    cost_by_arn = {r['Resource Id']: r for r in cost_rows}

    fixtures = metrics_fixture_lookup(args.fixtures_dir) if args.offline else None
    out = []
    account_id = args.account_id
    if not account_id:
        try:
            account_id = boto3.client('sts').get_caller_identity()['Account']
        except Exception:
            account_id = '000000000000'

    for region in regions:
        v2_list, classic_list = list_lbs(session, region, offline=args.offline, fixtures_dir=args.fixtures_dir)

        # ELBv2: ALB / NLB / Gateway
        for lb in v2_list:
            arn = lb['LoadBalancerArn']
            lb_type = lb.get('Type')  # application | network | gateway
            name = lb.get('LoadBalancerName', '')
            short_id = short_lb_id_from_arn(arn)

            idle = False
            if lb_type == 'application':
                req = cw_sum(session, region, 'AWS/ApplicationELB', 'RequestCount',
                             [{'Name': 'LoadBalancer', 'Value': short_id}],
                             look_start, look_end, 'Sum',
                             offline=args.offline, fixtures=fixtures)
                idle = (req <= 1)
            elif lb_type == 'network':
                bytes_sum = cw_sum(session, region, 'AWS/NetworkELB', 'ProcessedBytes',
                                   [{'Name': 'LoadBalancer', 'Value': short_id}],
                                   look_start, look_end, 'Sum',
                                   offline=args.offline, fixtures=fixtures)
                flows_avg = cw_avg(session, region, 'AWS/NetworkELB', 'ActiveFlowCount',
                                   [{'Name': 'LoadBalancer', 'Value': short_id}],
                                   look_start, look_end,
                                   offline=args.offline, fixtures=fixtures)
                idle = (bytes_sum <= 1024) and (flows_avg <= 0.01)
            elif lb_type == 'gateway':
                # Basic heuristic for GatewayLB (use ActiveFlowCount)
                flows_avg = cw_avg(session, region, 'AWS/GatewayELB', 'ActiveFlowCount',
                                   [{'Name': 'LoadBalancer', 'Value': short_id}],
                                   look_start, look_end,
                                   offline=args.offline, fixtures=fixtures)
                idle = (flows_avg <= 0.01)
            else:
                # Unknown type: skip
                continue

            if not idle:
                continue

            cost = cost_by_arn.get(arn)
            monthly_cost = float(cost['Monthly Cost']) if cost and cost.get('Monthly Cost') else 0.0
            annual_saving = int(round(monthly_cost * 12))
            if annual_saving < args.cost_threshold:
                continue

            out.append({
                'Account Id': cost['Account Id'] if cost else account_id,
                'Region': region,
                'Resource Id': arn,
                'Resource Name': name,
                'Scheme': lb.get('Scheme', ''),
                'Type': lb_type,
                'VPC Id': lb.get('VpcId', ''),
                'Availability Zones': azs_to_str(lb.get('AvailabilityZones', [])),
                'Created Time': to_iso(lb.get('CreatedTime')),
                'Security Groups': ','.join(lb.get('SecurityGroups', [])) if lb.get('SecurityGroups') else '',
                'Monthly Usage Hours': cost.get('Monthly Usage Hours', '') if cost else '',
                'Monthly Cost': monthly_cost,
                'Annual Saving': annual_saving
            })

        # Classic ELB
        for lb in classic_list:
            name = lb.get('LoadBalancerName', '')
            arn = arn_for_classic(name, region, account_id)
            req = cw_sum(session, region, 'AWS/ELB', 'RequestCount',
                         [{'Name': 'LoadBalancerName', 'Value': name}],
                         look_start, look_end, 'Sum',
                         offline=args.offline, fixtures=fixtures)
            if req > 1:
                continue

            cost = cost_by_arn.get(arn)
            monthly_cost = float(cost['Monthly Cost']) if cost and cost.get('Monthly Cost') else 0.0
            annual_saving = int(round(monthly_cost * 12))
            if annual_saving < args.cost_threshold:
                continue

            # Scheme heuristic for classic: if SecurityGroup exists, assume 'internal' otherwise internet-facing
            scheme = 'internal' if lb.get('SecurityGroups') else 'internet-facing'

            out.append({
                'Account Id': cost['Account Id'] if cost else account_id,
                'Region': region,
                'Resource Id': arn,
                'Resource Name': name,
                'Scheme': scheme,
                'Type': 'classic',
                'VPC Id': lb.get('VPCId', ''),
                'Availability Zones': ','.join(lb.get('AvailabilityZones', [])) if lb.get('AvailabilityZones') else '',
                'Created Time': to_iso(lb.get('CreatedTime')),
                'Security Groups': ','.join(lb.get('SecurityGroups', [])) if lb.get('SecurityGroups') else '',
                'Monthly Usage Hours': cost.get('Monthly Usage Hours', '') if cost else '',
                'Monthly Cost': monthly_cost,
                'Annual Saving': annual_saving
            })

    # Write CSV
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    fields = [
        'Account Id', 'Region', 'Resource Id', 'Resource Name', 'Scheme', 'Type', 'VPC Id',
        'Availability Zones', 'Created Time', 'Security Groups',
        'Monthly Usage Hours', 'Monthly Cost', 'Annual Saving'
    ]
    with open(args.output, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in out:
            w.writerow(r)

    print(f"Wrote {len(out)} idle LBs → {args.output}")


if __name__ == '__main__':
    main()
