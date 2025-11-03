#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Identify Idle Load Balancers (ALB/NLB/Classic) across one or many AWS accounts.

Multi-account scope options:
- --org-scan : list accounts from AWS Organizations, assume into each (read-only)
- --accounts-from-fixture <athena_cost.csv> : derive accounts from your cost fixture
- default: current account only

Dry-run:
- --list-accounts-only : print the final account set and exit (no CUR costs, no regions)

Idle definition over lookback window:
  * ALB: RequestCount Sum <= 1
  * NLB: ProcessedBytes Sum <= 1024 AND ActiveFlowCount Avg <= 0.01
  * GatewayLB: ActiveFlowCount Avg <= 0.01
  * Classic: RequestCount Sum <= 1

Costs source:
- Hybrid: --cost-from-fixture <path to athena_cost.csv>
- Online: Athena query (last month) when not offline and no fixture path given
- Offline: fixtures/athena_cost.csv when --offline is set
"""
import argparse
import csv
import datetime
import itertools
import os
import sys
import time
import json
from typing import Dict, List, Tuple, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ---------- Global client config ----------
CFG = Config(retries={'max_attempts': 10, 'mode': 'standard'})

# ---------- Utility helpers ----------

def load_account_map(path: Optional[str]) -> Dict[str, str]:
    m = {}
    if not path or not os.path.exists(path):
        return m
    with open(path, newline='') as f:
        for r in csv.DictReader(f):
            aid = r.get('Account Id') or r.get('AccountId') or r.get('Id')
            if not aid:
                continue
            m[str(aid)] = r.get('Account Name', '') or r.get('Name', '')
    return m

def load_cost_fixture(path: Optional[str]) -> List[dict]:
    rows = []
    if not path:
        return rows
    with open(path, newline='') as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows

def accounts_from_fixture(path: str) -> List[str]:
    ids = []
    seen = set()
    with open(path, newline='') as f:
        for r in csv.DictReader(f):
            aid = str(r.get('Account Id') or '').strip()
            if aid and aid not in seen:
                ids.append(aid); seen.add(aid)
    return ids

def all_regions(session, offline=False, fixtures_dir='fixtures') -> List[str]:
    if offline:
        # infer regions from local describe fixtures
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
    ec2 = session.client('ec2', config=CFG)
    return [r['RegionName'] for r in ec2.describe_regions(AllRegions=True)['Regions']
            if r.get('OptInStatus') in (None, 'opt-in-not-required', 'opted-in')]

def athena_query(session, q, db, workgroup, offline=False, fixtures_dir='fixtures'):
    if offline:
        # read local CSV fixture produced by cur_to_fixture.py
        path = os.path.join(fixtures_dir, 'athena_cost.csv')
        rows = []
        with open(path, newline='') as f:
            for r in csv.DictReader(f):
                rows.append(r)
        return rows
    # online Athena
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
    out_rows = []
    for r in res['ResultSet']['Rows'][1:]:
        row = {}
        for h, d in itertools.zip_longest(headers, r['Data']):
            row[h] = d.get('VarCharValue') if d else None
        out_rows.append(row)
    return out_rows

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
        key = dims[0]['Value']
        return float((fixtures or {}).get(key, {}).get(metric + stat, 0.0))
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
        return float((fixtures or {}).get(key, {}).get(metric + 'Average', 0.0))
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
    if ':loadbalancer/' in arn:
        return arn.split(':loadbalancer/')[1]
    return arn

def to_iso(val):
    if not val:
        return ''
    if isinstance(val, str):
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

# ---------- Multi-account helpers ----------

def get_base_identity() -> Tuple[str, str]:
    c = boto3.client('sts')
    r = c.get_caller_identity()
    return r['Account'], r['Arn']

def list_org_accounts() -> List[dict]:
    org = boto3.client('organizations')
    out = []
    token = None
    while True:
        resp = org.list_accounts(NextToken=token) if token else org.list_accounts()
        out.extend(resp['Accounts'])
        token = resp.get('NextToken')
        if not token:
            break
    return [{'Id': a['Id'], 'Name': a['Name'], 'Email': a.get('Email',''), 'Status': a.get('Status','ACTIVE')} for a in out]

def assume_into(account_id: str, role_name: str, session_name='thecostfather-scan') -> boto3.Session:
    arn = f"arn:aws:iam::{account_id}:role/{role_name}"
    sts = boto3.client('sts')
    creds = sts.assume_role(RoleArn=arn, RoleSessionName=session_name)['Credentials']
    return boto3.Session(
        aws_access_key_id=creds['AccessKeyId'],
        aws_secret_access_key=creds['SecretAccessKey'],
        aws_session_token=creds['SessionToken'],
    )

def filter_accounts(ids: List[str], include: Optional[str], exclude: Optional[str]) -> List[str]:
    def to_set(s):
        return set([x.strip() for x in s.split(',') if x.strip()]) if s else set()
    inc = to_set(include)
    exc = to_set(exclude)
    out=[]
    for i in ids:
        if inc and i not in inc:
            continue
        if i in exc:
            continue
        out.append(i)
    return out

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(description="Identify idle ELBs (ALB/NLB/Classic) across one or many AWS accounts")
    # Discovery / scope
    ap.add_argument('--org-scan', action='store_true', help='List accounts from AWS Organizations and scan them (assumes role in each)')
    ap.add_argument('--assume-role-name', default='OrganizationAccountAccessRole', help='Role name to assume per account when --org-scan is set')
    ap.add_argument('--accounts-from-fixture', default=None, help='CSV path (athena_cost.csv) to infer account IDs from costs')
    ap.add_argument('--include-accounts', default=None, help='Comma-separated Account IDs to include (whitelist)')
    ap.add_argument('--exclude-accounts', default=None, help='Comma-separated Account IDs to exclude (blacklist)')
    ap.add_argument('--list-accounts-only', action='store_true', help='Only list accounts that would be scanned, then exit')
    ap.add_argument('--account-map', default='fixtures/accounts.csv', help='CSV with Account Id,Account Name (for pretty names)')
    # Regions / time / costs
    ap.add_argument('--regions', nargs='*', help='Subset of regions (default: discover all in each account)')
    ap.add_argument('--lookback-hours', type=int, default=336, help='Hours to analyze for zero traffic')
    ap.add_argument('--cost-threshold', type=float, default=0.0, help='Min annual saving ($) to include')
    ap.add_argument('--athena-db', default='wivdb')
    ap.add_argument('--athena-table', default='wiv_cur')
    ap.add_argument('--workgroup', default='primary')
    ap.add_argument('--cost-from-fixture', default=None, help='Local CSV with costs (athena_cost.csv). If set, do NOT query Athena.')
    # Offline fixtures
    ap.add_argument('--offline', action='store_true', help='Use local fixtures for everything')
    ap.add_argument('--fixtures-dir', default='fixtures', help='Fixtures dir for offline mode')
    # Output
    ap.add_argument('--output', default='out/idle_elb.csv')
    # Classic helpers
    ap.add_argument('--account-id', default=None, help='Override for classic ARN construction (single-account mode)')
    args = ap.parse_args()

    # Sanity-check credentials up front
    try:
        base_acct, base_arn = get_base_identity()
        print(f"[INFO] Using base credentials: {base_acct} as {base_arn}")
    except Exception:
        sys.exit(
            "AWS credentials are invalid/expired for this shell.\n"
            "Try: unset AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_SESSION_TOKEN, "
            "export AWS_PROFILE=<name>, and 'aws sso login --profile <name>' if using SSO."
        )

    # Pretty names
    acct_names = load_account_map(args.account_map)

    # ---- Build account list FIRST (no CUR costs yet) ----
    target_accounts: List[str] = []

    if args.org_scan:
        try:
            org_accounts = list_org_accounts()
            target_accounts = [a['Id'] for a in org_accounts if a.get('Status','ACTIVE') == 'ACTIVE']
            if base_acct not in target_accounts:
                target_accounts.append(base_acct)
        except ClientError as e:
            sys.exit(f"[ERROR] Organizations access failed: {e}")

    elif args.accounts_from_fixture:
        target_accounts = accounts_from_fixture(args.accounts_from_fixture)
        if not target_accounts:
            print("[WARN] No accounts found in fixture, falling back to current account only.")
            target_accounts = [base_acct]
    else:
        target_accounts = [args.account_id] if args.account_id else [base_acct]

    # include/exclude filters
    target_accounts = filter_accounts(target_accounts, args.include_accounts, args.exclude_accounts)

    # De-duplicate & sort
    target_accounts = sorted(set(target_accounts), key=lambda x: x)

    # Show and optionally exit
    print(f"[INFO] Accounts to scan ({len(target_accounts)}):")
    for aid in target_accounts:
        nm = acct_names.get(aid, '')
        print(f"  - {aid}{'  ('+nm+')' if nm else ''}")
    if args.list_accounts_only:
        return

    # ---- NOW load costs (only because we will scan) ----
    if args.cost_from_fixture:
        cost_rows = load_cost_fixture(args.cost_from_fixture)
    else:
        sql_path = os.path.join('sql', 'elb_cost_last_month.sql')
        if not args.offline:
            if not os.path.exists(sql_path):
                sys.exit(f"Missing SQL file: {sql_path}")
            sql = open(sql_path, 'r').read() \
                .replace('${athena_db}', args.athena_db) \
                .replace('${athena_table}', args.athena_table)
            cost_rows = athena_query(boto3.Session(), sql, args.athena_db, args.workgroup,
                                     offline=False, fixtures_dir=args.fixtures_dir)
        else:
            cost_rows = athena_query(boto3.Session(), None, None, None,
                                     offline=True, fixtures_dir=args.fixtures_dir)
    cost_by_arn = {r['Resource Id']: r for r in cost_rows}

    # Time window
    look_end = datetime.datetime.utcnow()
    look_start = look_end - datetime.timedelta(hours=args.lookback_hours)

    fixtures = metrics_fixture_lookup(args.fixtures_dir) if args.offline else None
    all_rows = []
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    # For each account, create a session (assume if needed), then scan regions
    for aid in target_accounts:
        if args.org_scan and aid != base_acct:
            try:
                acct_sess = assume_into(aid, args.assume_role_name)
                print(f"[INFO] Assumed into {aid} via role {args.assume_role_name}")
            except ClientError as e:
                print(f"[WARN] Skipping {aid}: cannot assume role {args.assume_role_name}: {e}")
                continue
        else:
            acct_sess = boto3.Session()

        # Regions for this account
        regions = args.regions or all_regions(acct_sess, offline=args.offline, fixtures_dir=args.fixtures_dir)
        print(f"[INFO] {aid}: scanning {len(regions)} regions")

        seen = set()  # (region, arn)

        # ----- ELBv2 -----
        for region in regions:
            try:
                v2_list, classic_list = list_lbs(acct_sess, region, offline=args.offline, fixtures_dir=args.fixtures_dir)
            except ClientError as e:
                print(f"[WARN] {aid}:{region}: describe_load_balancers failed: {e}")
                continue

            for lb in v2_list:
                arn = lb.get('LoadBalancerArn')
                if not arn:
                    continue
                key = (region, arn)
                if key in seen:
                    continue
                seen.add(key)

                state_code = (lb.get('State') or {}).get('Code')
                if state_code and state_code != 'active':
                    continue

                lb_type = lb.get('Type')
                name = lb.get('LoadBalancerName', '')
                short_id = short_lb_id_from_arn(arn)

                idle = False
                try:
                    if lb_type == 'application':
                        req = cw_sum(acct_sess, region, 'AWS/ApplicationELB', 'RequestCount',
                                     [{'Name': 'LoadBalancer', 'Value': short_id}],
                                     look_start, look_end, 'Sum', offline=args.offline, fixtures=fixtures)
                        idle = (req <= 1)
                    elif lb_type == 'network':
                        bytes_sum = cw_sum(acct_sess, region, 'AWS/NetworkELB', 'ProcessedBytes',
                                           [{'Name': 'LoadBalancer', 'Value': short_id}],
                                           look_start, look_end, 'Sum', offline=args.offline, fixtures=fixtures)
                        flows_avg = cw_avg(acct_sess, region, 'AWS/NetworkELB', 'ActiveFlowCount',
                                           [{'Name': 'LoadBalancer', 'Value': short_id}],
                                           look_start, look_end, offline=args.offline, fixtures=fixtures)
                        idle = (bytes_sum <= 1024) and (flows_avg <= 0.01)
                    elif lb_type == 'gateway':
                        flows_avg = cw_avg(acct_sess, region, 'AWS/GatewayELB', 'ActiveFlowCount',
                                           [{'Name': 'LoadBalancer', 'Value': short_id}],
                                           look_start, look_end, offline=args.offline, fixtures=fixtures)
                        idle = (flows_avg <= 0.01)
                    else:
                        continue
                except ClientError as e:
                    print(f"[WARN] {aid}:{region}:{name}: metrics lookup failed: {e}")
                    continue

                if not idle:
                    continue

                cost = cost_by_arn.get(arn)
                monthly_cost = float(cost['Monthly Cost']) if cost and cost.get('Monthly Cost') else 0.0
                annual_saving = int(round(monthly_cost * 12))
                if annual_saving < args.cost_threshold:
                    continue

                all_rows.append({
                    'Account Id': aid if not (cost and cost.get('Account Id')) else str(cost['Account Id']),
                    'Account Name': acct_names.get(aid, ''),
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

            # ----- Classic ELB -----
            for lb in classic_list:
                name = lb.get('LoadBalancerName', '')
                if not name:
                    continue
                arn = arn_for_classic(name, region, aid)
                key = (region, arn)
                if key in seen:
                    continue
                seen.add(key)

                try:
                    req = cw_sum(acct_sess, region, 'AWS/ELB', 'RequestCount',
                                 [{'Name': 'LoadBalancerName', 'Value': name}],
                                 look_start, look_end, 'Sum', offline=args.offline, fixtures=fixtures)
                except ClientError as e:
                    print(f"[WARN] {aid}:{region}:{name}: metrics lookup failed: {e}")
                    continue

                if req > 1:
                    continue

                cost = cost_by_arn.get(arn)
                monthly_cost = float(cost['Monthly Cost']) if cost and cost.get('Monthly Cost') else 0.0
                annual_saving = int(round(monthly_cost * 12))
                if annual_saving < args.cost_threshold:
                    continue

                scheme = 'internal' if lb.get('SecurityGroups') else 'internet-facing'

                all_rows.append({
                    'Account Id': aid if not (cost and cost.get('Account Id')) else str(cost['Account Id']),
                    'Account Name': acct_names.get(aid, ''),
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
    fields = [
        'Account Id','Account Name','Region','Resource Id','Resource Name','Scheme','Type','VPC Id',
        'Availability Zones','Created Time','Security Groups',
        'Monthly Usage Hours','Monthly Cost','Annual Saving'
    ]
    with open(args.output, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in all_rows:
            w.writerow(r)

    print(f"Wrote {len(all_rows)} idle LBs → {args.output}")

if __name__ == '__main__':
    main()
