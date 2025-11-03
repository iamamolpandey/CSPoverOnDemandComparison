# CSPoverOnDemandComparison
This repo script will help you to find savings on Compute savings plan over ondemand AWS compute rates.


## Pre-requisite 👍
- Python should be installed on system (for Linux/Ubuntu machines already present)
- A CSV file with at least columns Region, Instance Type, Operating System	, Quantity(Hrs) (or directly download the CK-Lens Cost Breakup file)

## How to run:
clone the script directly on your machine.
- In the same folder, put the csv file.
- Run the script using this command.
  python3 script.py csv_filename.csv
- It will take time based on the number of rows in your csv file. After the process completes it will generate an Output file (with name Output.csv). Where you can find new columns like, On demand Rate, CSP rate, total OD Cost, Total CSP Cost, Savings. Along with existing columns.
