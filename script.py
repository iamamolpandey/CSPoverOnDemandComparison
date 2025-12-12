#!/usr/bin/env python3
"""
AWS EC2 Savings Plans Calculator - Optimized Version 1.1
Features: Multithreading (12 workers), Caching, Rate Limiting
"""
import boto3
import json
import csv
import time
import threading
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from collections import deque
import sys
from datetime import datetime

AWS_REGION = "us-east-1"

# Region prefix mapping for Savings Plans API
REGION_TO_USAGE_PREFIX = {
    "us-east-1": "USE1", "us-east-2": "USE2", "us-west-1": "USW1", "us-west-2": "USW2",
    "ca-central-1": "CAN1", "ca-west-1": "CAN2", "sa-east-1": "SAE1", "mx-central-1": "MXC1",
    "eu-west-1": "EU", "eu-west-2": "EU", "eu-west-3": "EU", "eu-central-1": "EU",
    "eu-north-1": "EU", "eu-south-1": "EU", "eu-south-2": "EU", "eu-central-2": "EU",
    "ap-south-1": "APS1", "ap-south-2": "APS2", "ap-southeast-1": "APS1", "ap-southeast-2": "APS2", 
    "ap-southeast-3": "APS3", "ap-southeast-4": "APS4", "ap-southeast-5": "APS5", "ap-southeast-6": "APS6",
    "ap-southeast-7": "APS7", "ap-northeast-1": "APN1", "ap-northeast-2": "APN2", "ap-northeast-3": "APN3",
    "ap-east-1": "APE1", "ap-east-2": "APE2", "il-central-1": "ILC1", "me-south-1": "MES1", "me-central-1": "MEC1", "af-south-1": "AFS1",
    # Local Zones (inherit parent region prefix)
    "us-west-2-den-1a": "USW2",
    "us-east-1-atl-1a": "USE1",
    "us-east-1-bos-1a": "USE1",
    "us-east-1-chi-1a": "USE1",
    "us-east-1-dfw-1a": "USE1",
    "us-east-1-iah-1a": "USE1",
    "us-west-2-lax-1a": "USW2",
    "us-east-1-mia-1a": "USE1",
    "us-east-1-nyc-1a": "USE1"
}

# Region name to code mapping
REGION_MAP = {
    "Oregon": "us-west-2", "Mumbai": "ap-south-1", "Hyderabad": "ap-south-2", "Ireland": "eu-west-1",
    "N. Virginia": "us-east-1", "Ohio": "us-east-2", "N. California": "us-west-1",
    "Frankfurt": "eu-central-1", "London": "eu-west-2", "Paris": "eu-west-3",
    "Stockholm": "eu-north-1", "Milan": "eu-south-1", "Spain": "eu-south-2", "Zurich": "eu-central-2",
    "Singapore": "ap-southeast-1", "Sydney": "ap-southeast-2", "Jakarta": "ap-southeast-3",
    "Melbourne": "ap-southeast-4", "Malaysia": "ap-southeast-5", "New Zealand": "ap-southeast-6",
    "Thailand": "ap-southeast-7", "Tokyo": "ap-northeast-1",
    "Seoul": "ap-northeast-2", "Osaka": "ap-northeast-3", "Hong Kong": "ap-east-1", "Taipei": "ap-east-2",
    "Montreal": "ca-central-1", "Calgary": "ca-west-1", "São Paulo": "sa-east-1", "Bahrain": "me-south-1",
    "UAE": "me-central-1", "Israel": "il-central-1", "Mexico": "mx-central-1", "Cape Town": "af-south-1",
    # Local Zones
    "Denver": "us-west-2-den-1a", "Atlanta": "us-east-1-atl-1a", "Boston": "us-east-1-bos-1a",
    "Chicago": "us-east-1-chi-1a", "Dallas": "us-east-1-dfw-1a", "Houston": "us-east-1-iah-1a",
    "Los Angeles": "us-west-2-lax-1a", "Miami": "us-east-1-mia-1a", "New York City": "us-east-1-nyc-1a",
}

# Rate limiter class
class RateLimiter:
    def __init__(self, max_calls_per_second):
        self.max_calls = max_calls_per_second
        self.calls = deque()
        self.lock = threading.Lock()
    
    def acquire(self):
        with self.lock:
            now = time.time()
            while self.calls and self.calls[0] < now - 1.0:
                self.calls.popleft()
            if len(self.calls) >= self.max_calls:
                sleep_time = self.calls[0] + 1.0 - now
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    while self.calls and self.calls[0] < now - 1.0:
                        self.calls.popleft()
            self.calls.append(time.time())

# Thread-safe cache class
class PricingCache:
    def __init__(self):
        self.cache = {}
        self.lock = threading.Lock()
        self.hits = 0
        self.misses = 0
    
    def get_key(self, *args):
        data = ":".join(str(arg) for arg in args)
        return hashlib.md5(data.encode()).hexdigest()
    
    def get(self, key):
        with self.lock:
            if key in self.cache:
                self.hits += 1
                return self.cache[key]
            self.misses += 1
            return None
    
    def set(self, key, value):
        with self.lock:
            self.cache[key] = value
    
    def stats(self):
        with self.lock:
            total = self.hits + self.misses
            return {
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": (self.hits / total * 100) if total > 0 else 0,
                "size": len(self.cache)
            }

# Global instances
pricing_limiter = RateLimiter(8)  # 8 RPS for Pricing API
sp_limiter = RateLimiter(4)       # 4 RPS for Savings Plans API
cache = PricingCache()

# Metrics tracking
metrics_lock = threading.Lock()
metrics = {"processed": 0, "api_calls": 0, "errors": 0, "throttles": 0}

def log_metric(key, value=1):
    with metrics_lock:
        metrics[key] += value

def validate_aws_credentials():
    """Check if AWS credentials are valid"""
    try:
        boto3.client('sts').get_caller_identity()
        return True
    except (ClientError, Exception):
        return False

def validate_csv_headers(headers):
    """Check if CSV has required columns"""
    required = ["Instance Type", "Region", "Operating System", "Quantity(Hrs)"]
    missing = [col for col in required if col not in headers]
    return missing

def normalize_region(region):
    region_code = REGION_MAP.get(region, region)
    if region_code and '-' in region_code and len(region_code.split('-')) > 3:
        return '-'.join(region_code.split('-')[:3])  # Extract parent for Local Zones
    return region_code

def normalize_os(os_name):
    os_name = os_name.strip().lower()
    if "linux" in os_name:
        return "Linux"
    elif "windows" in os_name:
        return "Windows"
    elif "rhel" in os_name:
        return "RHEL"
    return "Linux"

def normalize_os_for_sp(os_name):
    os_name = os_name.strip().lower()
    if "linux" in os_name:
        return "Linux/UNIX"
    elif "windows" in os_name:
        return "Windows"
    elif "rhel" in os_name:
        return "Red Hat Enterprise Linux"
    return "Linux/UNIX"

def get_od_price(instance_type, region, os_type):
    """Get On-Demand price with caching and rate limiting"""
    cache_key = cache.get_key("od", instance_type, region, os_type)
    cached = cache.get(cache_key)
    if cached:
        return cached
    
    pricing_limiter.acquire()
    
    try:
        client = boto3.client("pricing", region_name=AWS_REGION)
        resp = client.get_products(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Field": "instanceType", "Value": instance_type, "Type": "TERM_MATCH"},
                {"Field": "regionCode", "Value": region, "Type": "TERM_MATCH"},
                {"Field": "tenancy", "Value": "Shared", "Type": "TERM_MATCH"},
                {"Field": "operatingSystem", "Value": os_type, "Type": "TERM_MATCH"},
                {"Field": "preInstalledSw", "Value": "NA", "Type": "TERM_MATCH"},
                {"Field": "capacitystatus", "Value": "Used", "Type": "TERM_MATCH"}
            ]
        )
        
        if not resp["PriceList"]:
            raise ValueError(f"No price found for {instance_type} in {region}")
        
        item = json.loads(resp["PriceList"][0])
        term = list(item["terms"]["OnDemand"].values())[0]
        rate = float(list(term["priceDimensions"].values())[0]["pricePerUnit"]["USD"])
        
        cache.set(cache_key, rate)
        log_metric("api_calls")
        return rate
        
    except ClientError as e:
        if 'Throttling' in str(e):
            log_metric("throttles")
        raise

def get_sp_rate(region, instance_type, os_type):
    """Get Savings Plans rate with caching and rate limiting"""
    cache_key = cache.get_key("sp", instance_type, region, os_type)
    cached = cache.get(cache_key)
    if cached:
        return cached
    
    region_prefix = REGION_TO_USAGE_PREFIX.get(region, "USE1")
    if region_prefix == "USE1":
        usage_type = f"BoxUsage:{instance_type}"
    else:
        usage_type = f"{region_prefix}-BoxUsage:{instance_type}"
    
    sp_limiter.acquire()
    
    try:
        client = boto3.client('savingsplans', region_name=region)
        
        # Get offering ID
        resp1 = client.describe_savings_plans_offerings(
            paymentOptions=['No Upfront'],
            productType='EC2',
            planTypes=['Compute'],
            durations=[31536000],
            maxResults=1
        )
        
        if not resp1.get('searchResults'):
            raise Exception("No SP offerings found")
        
        offering_id = resp1['searchResults'][0]['offeringId']
        # Get rates
        resp2 = client.describe_savings_plans_offering_rates(
            savingsPlanOfferingIds=[offering_id],
            savingsPlanPaymentOptions=['No Upfront'],
            savingsPlanTypes=['Compute'],
            products=['EC2'],
            serviceCodes=['AmazonEC2'],
            usageTypes=[usage_type],
            filters=[
                {"name": "instanceType", "values": [instance_type]},
                {"name": "productDescription", "values": [os_type]}
            ],
            maxResults=10
        )
        
        rates = resp2.get('searchResults', [])
        if not rates:
            raise ValueError(f"No SP rates for {instance_type} in {region}")
        
        rate = float(rates[0]['rate'])
        cache.set(cache_key, rate)
        log_metric("api_calls", 2)
        return rate
        
    except ClientError as e:
        if 'Throttling' in str(e):
            log_metric("throttles")
        raise

def process_row(row_data, row_idx):
    """Process single row with caching"""
    try:
        instance_type = row_data["Instance Type"].strip()
        region_display = row_data["Region"].strip()
        os_raw = row_data["Operating System"]
        hours = float(row_data["Quantity(Hrs)"])
        
        region = normalize_region(region_display)
        os_type = normalize_os(os_raw)
        os_type_sp = normalize_os_for_sp(os_raw)
        
        od_rate = get_od_price(instance_type, region, os_type)
        sp_rate = get_sp_rate(region, instance_type, os_type_sp)
        
        total_od = round(od_rate * hours, 4)
        total_sp = round(sp_rate * hours, 4)
        savings = round(total_od - total_sp, 4)
        
        log_metric("processed")
        
        return {
            "success": True,
            "od_rate": od_rate,
            "sp_rate": sp_rate,
            "total_od": total_od,
            "total_sp": total_sp,
            "savings": savings
        }
        
    except Exception as e:
        log_metric("errors")
        return {"success": False, "error": str(e)}

def process_csv(input_file, output_file="output.csv"):
    """Main processing function with multithreading"""
    print("="*70)
    print("AWS SAVINGS PLANS CALCULATOR v1.2")
    print("="*70)
    
    # 1. Validate AWS Credentials
    print("Checking AWS credentials...", end=" ")
    if not validate_aws_credentials():
        print("FAILED")
        print("-"*70)
        print("ERROR: Unable to locate valid AWS credentials.")
        print("RECOMMENDATION: Run 'aws configure' to set up your credentials.")
        print("-"*70)
        sys.exit(1)
    print("OK")

    print(f"Input:  {input_file}")
    print(f"Output: {output_file}")
    print(f"Threads: 12")
    print(f"Features: Caching + Rate Limiting")
    print("="*70 + "\n")
    
    # 2. Load CSV with Error Handling
    try:
        with open(input_file, 'r') as f:
            reader = list(csv.reader(f))
            if not reader:
                print(f"ERROR: File '{input_file}' is empty.")
                sys.exit(1)
    except FileNotFoundError:
        print(f"ERROR: Input file '{input_file}' not found.")
        print(f"RECOMMENDATION: Check the file path and name.")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to read file '{input_file}': {str(e)}")
        sys.exit(1)
    
    headers = reader[0]
    rows = reader[1:]
    
    # 3. Validate CSV Headers
    missing_cols = validate_csv_headers(headers)
    if missing_cols:
        print(f"ERROR: Input CSV missing required columns: {', '.join(missing_cols)}")
        print(f"REQUIRED: Instance Type, Region, Operating System, Quantity(Hrs)")
        sys.exit(1)
    
    print(f"Rows to process: {len(rows)}\n")
    
    start_time = time.time()
    
    # Process with multithreading
    results = [None] * len(rows)
    with ThreadPoolExecutor(max_workers=12) as executor:
        row_dicts = [dict(zip(headers, row)) for row in rows]
        futures = {executor.submit(process_row, row_dict, i): i for i, row_dict in enumerate(row_dicts)}
        
        completed = 0
        for future in as_completed(futures):
            idx = futures[future]
            result = future.result()
            results[idx] = result
            completed += 1
            
            if completed % 100 == 0 or completed == len(rows):
                elapsed = time.time() - start_time
                rate = completed / elapsed
                eta = (len(rows) - completed) / rate if rate > 0 else 0
                print(f"  Progress: {completed}/{len(rows)} ({completed/len(rows)*100:.1f}%) "
                      f"| Rate: {rate:.1f} rows/s | ETA: {eta:.0f}s")
    
    duration = time.time() - start_time
    
    # Build output CSV
    new_headers = headers + ["OnDemand_Rate", "ComputeSP_Rate", "Total_OnDemand", "Total_CSP", "Savings", "Error_Message"]
    output_rows = []
    
    for i, (row, result) in enumerate(zip(rows, results)):
        if result["success"]:
            output_rows.append(row + [
                result["od_rate"], result["sp_rate"],
                result["total_od"], result["total_sp"], result["savings"],
                "" # Empty error message
            ])
        else:
            # Add error details to the row
            output_rows.append(row + ["", "", "", "", "", result["error"]])
    
    # Write output
    try:
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(new_headers)
            writer.writerows(output_rows)
    except Exception as e:
        print(f"\nERROR: Failed to write to output file '{output_file}': {str(e)}")
        print("RECOMMENDATION: Check if the file is open in another program.")
        sys.exit(1)
    
    # Print summary
    cache_stats = cache.stats()
    
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"Duration:         {duration:.2f}s ({duration/60:.2f} minutes)")
    print(f"Rows Processed:   {metrics['processed']}")
    print(f"Errors:           {metrics['errors']}")
    print(f"Processing Rate:  {len(rows)/duration:.2f} rows/second")
    print(f"\nAPI Performance:")
    print(f"  API Calls:      {metrics['api_calls']}")
    print(f"  Throttles:      {metrics['throttles']}")
    print(f"\nCache Performance:")
    print(f"  Cache Hits:     {cache_stats['hits']}")
    print(f"  Cache Misses:   {cache_stats['misses']}")
    print(f"  Hit Rate:       {cache_stats['hit_rate']:.1f}%")
    print(f"  Cache Size:     {cache_stats['size']} unique items")
    print(f"  Calls Saved:    {cache_stats['hits'] * 3} API calls")
    print(f"\n✅ Results saved to: {output_file}")
    if metrics['errors'] > 0:
        print(f"⚠️  {metrics['errors']} rows failed. Check 'Error_Message' column in output.")
    print(f"{'='*70}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else "output.csv"
    else:
        input_file = "test_500.csv"
        output_file = "output_500.csv"
    
    process_csv(input_file, output_file)
