#!/usr/bin/env python3
# Print unique regions from a CUR-derived cost fixture (athena_cost.csv).
# A region is included if Monthly Cost >= threshold OR Monthly Usage Hours > 0.

import argparse, csv

def main():
    ap = argparse.ArgumentParser(description="List regions present in athena_cost.csv")
    ap.add_argument('--fixture', default='fixtures/athena_cost.csv',
                    help='Path to cost fixture CSV (default: fixtures/athena_cost.csv)')
    ap.add_argument('--min-monthly-cost', type=float, default=0.0,
                    help='Only include regions with Monthly Cost >= this value (default: 0.0)')
    args = ap.parse_args()

    regions = set()
    with open(args.fixture, newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            region = (r.get('Region') or '').strip()
            if not region:
                continue
            try:
                cost = float(r.get('Monthly Cost', '0') or 0)
            except:
                cost = 0.0
            try:
                usage = float(r.get('Monthly Usage Hours', '0') or 0)
            except:
                usage = 0.0
            if cost >= args.min_monthly_cost or usage > 0:
                regions.add(region)

    print(' '.join(sorted(regions)))

if __name__ == '__main__':
    main()
