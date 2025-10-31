Identify Idle Load Balancer
Elastic Load Balancing - Idle ELB - https://catalog.workshops.aws/cur-query-library/en-US/queries/cost-optimization#elastic-load-balancing-idle-elb


This analysis identifies Elastic Load Balancers (ELBs) that received no traffic in the past month while running longer than a specified period. It provides cost and usage data for these idle ELBs, enabling informed decisions about potential resource decommissioning or reconfiguration to optimize cloud infrastructure and reduce unnecessary expenses.




Trigger Parameters 
lookback_period - Idle hours threshold (e.g., 336 hours)cost_threshold - Minimum amount to create a case [0$]

# use the profile you pasted in ~/.aws/credentials
export AWS_PROFILE=890769921003_AdministratorAccess


sudo apt-get update && sudo apt-get install -y python3 python3-venv python3-pip

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel

pip install -r requirements.txt

# aws s3 sync s3://aws-map-cur-bucket-xebiaprod-01/cur-export/aws-map-cur-export-xebiaprod-01 cur_raw/


# inside your venv
pip install duckdb>=1.0.0


python tools/cur_to_fixture.py --cur-dir cur_raw --fixtures-dir fixtures --synthesize-lb-fixtures


python tools/identify_idle_elb.py \
  --regions eu-west-1 \
  --cost-from-fixture fixtures/athena_cost.csv \
  --account-map fixtures/accounts.csv \
  --lookback-hours 336 \
  --cost-threshold 0 \
  --output out/idle_elb_online.csv
Wrote 0 idle LBs → out/idle_elb_online.csv

