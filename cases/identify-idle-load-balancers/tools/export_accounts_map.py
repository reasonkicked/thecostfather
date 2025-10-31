#!/usr/bin/env python
# Export Organizations account list to fixtures/accounts.csv (read-only)
import boto3, csv
org = boto3.client('organizations')
acc = []
token = None
while True:
    r = org.list_accounts(NextToken=token) if token else org.list_accounts()
    acc += r['Accounts']; token = r.get('NextToken')
    if not token: break
with open('fixtures/accounts.csv','w',newline='') as f:
    w = csv.writer(f)
    w.writerow(['Account Id','Account Name'])
    for a in acc:
        w.writerow([a['Id'], a['Name']])
print(f"Wrote fixtures/accounts.csv with {len(acc)} accounts")
