# CSPoverOnDemandComparison
This repo script will help you to find savings on Compute savings plan over ondemand AWS compute rates.


## Pre-requisite 👍
- Python should be installed on system (for Linux/Ubuntu machines already present)
- A CSV file with at least columns Region, Instance Type, Operating System	, Quantity(Hrs) (or directly download the CK-Lens Cost Breakup file)



# �� Windows Setup - Script 1.1
---
## Step 1: Install AWS CLI
Download and install:
```
https://awscli.amazonaws.com/AWSCLIV2.msi
```
Run installer → Next → Next → Install → Finish

Verify (open CMD):
```cmd
aws --version
```
---
## Step 2: Get AWS Access Keys ( if you dont have already)

1. Login to AWS Console: https://console.aws.amazon.com/
2. Click your username (top right) → **Security credentials**
3. Scroll to **Access keys** → Click **Create access key**
4. Select **Command Line Interface (CLI)** → Check confirmation → **Next**
5. Click **Create access key**
6. **Copy** the Access Key ID and Secret Access Key
7. Download the `.csv` file (important - you can't see secret again!)

---
## Step 3: Configure AWS CLI

```cmd
aws configure
```
Enter when prompted:
```
AWS Access Key ID: [paste your key]
AWS Secret Access Key: [paste your secret]
Default region name: us-east-1
Default output format: json
```
Verify it works:
```cmd
aws sts get-caller-identity
```
You should see your account info.

---
## Step 4: Install Boto3

```cmd
pip install boto3
```

---
## Step 5: Run the Script

```cmd
python script.py input.csv output.csv
```
It will take time based on the number of rows in your csv file. After the process completes it will generate an Output file (with name Output.csv). Where you can find new columns like, On demand Rate, CSP rate, total OD Cost, Total CSP Cost, Savings. Along with existing columns.
Done! Results saved to `output.csv`

---
