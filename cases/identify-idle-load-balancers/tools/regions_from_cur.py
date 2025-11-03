#!/usr/bin/env python
import argparse, csv, sys
def main():
    ap = argparse.ArgumentParser(description="List regions present in athena_cost.csv")
    ap.add_argument('--cost-fixture', default='fixtures/athena_cost.csv')
    ap.add_argument('--min-monthly-cost', type=float, default=0.0, help='Filter regions where total cost >= this')
    args = ap.parse_args()

    per_region = {}
    with open(args.cost_fixture, newline='') as f:
        for r in csv.DictReader(f):
            reg = (r.get('Region') or '').strip()
            if not reg:
                continue
            per_region.setdefault(reg, 0.0)
            try:
                per_region[reg] += float(r.get('Monthly Cost') or 0.0)
            except:
                pass

    regs = [r for r,c in per_region.items() if c >= args.min_monthly_cost]
    regs.sort()
    print(' '.join(regs))
if __name__ == '__main__':
    main()
