# Airline Data and Stock Data Download

This repository contains Python scripts, shell scripts, and instructions to set up an environment for extracting, transforming, and loading airline data and stock market trends data into an Amazon S3 bucket. These datasets can be used for building Machine Learning models and dashboards. The project leverages tools like `selenium`, `yfinance`, and `pandas` for data extraction and processing.

---

## Table of Contents
- [Automation Overview](#automation-overview)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
  - [Step 1: Clone the Repository](#step-1-clone-the-repository)
  - [Step 2: Update System Packages](#step-2-update-system-packages)
  - [Step 3: Install Required Tools](#step-3-install-required-tools)
  - [Step 4: Install Python Packages](#step-4-install-python-packages)
  - [Step 5: Run the Scripts](#step-5-run-the-scripts)
  - [Step 6: Copy Files to S3](#step-6-copy-files-to-s3)
  - [Step 7: Set Up AWS Glue Jobs](#step-7-set-up-aws-glue-jobs)
- [Directory Structure](#directory-structure)
- [License](#license)

---

## Automation Overview

The goals of this project are:
1. To extract airline data from various sources.
2. To retrieve stock market data for analysis using Python.
3. To store processed data for further analysis in an S3 bucket.

The automation is designed to run on an Amazon EC2 instance with Amazon Linux 2023.

---

## Prerequisites

Before starting, ensure you have the following:
- An Amazon EC2 instance running Amazon Linux 2023.
- Python 3.9 or higher installed on the instance.
- AWS CLI configured with proper credentials and permissions to interact with S3.
- Basic knowledge of Python, shell scripting, and Linux commands.
- An AWS S3 bucket for storing processed files.

---

## Setup Instructions

### Step 1: Clone the Repository
Clone the repository to your EC2 instance:
```bash
git clone https://github.com/your-username/airline-data-analysis.git
cd airline-data-analysis
```

### Step 2: Update System Packages
Update the system packages to ensure you have the latest versions:
```bash
sudo dnf update -y
```

### Step 3: Install Required Tools
1. **Install pip**:
```bash
sudo yum install python3-pip -y
```

2. **Install Google Chrome for EC2**:
```bash
sudo wget https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm
sudo yum install ./google-chrome-stable_current_x86_64.rpm -y
```

### Step 4: Install Python Packages
Install the necessary Python libraries using `pip`:
```bash
pip3 install selenium webdriver-manager yfinance==0.2.44 pandas boto3
```

### Step 5: Run the Scripts
Execute the Python scripts to download the required data:
1. **Run the airline data script**:
```bash
python3 automation/btsdatapull.py
```

2. **Run the stock data script**:
```bash
python3 automation/stockdatapull.py
```

### Step 6: Copy Files to S3
Use the shell script to upload the downloaded files to your S3 bucket:
```bash
bash automation/s3_upload.sh
```
After successful execution, the files will be uploaded to the specified S3 bucket.

---

## Directory Structure

After running the scripts, the directory structure should look like this:

```bash
airline-data-analysis/
├── automation/
│   ├── btsdatapull.py
│   ├── stockdatapull.py
│   ├── s3_upload.sh
│   ├── AWS Glue ETL Job for Form 41 Data Processing.py
│   ├── AWS Glue ETL Job for DataCleanAggregate.py
│   ├───data/
│   │   ├── airline_stock_data.csv
│   │   ├── stock_data_collection.log
│   ├───downloads/
│   │   ├── T_F41SCHEDULE_B1.csv
│   │   ├── T_F41SCHEDULE_B11.csv
│   │   ├── ...
│   │   ├── T_SCHEDULE_T2.csv
```

### Local Source Folders
1. **Form 41 Data**:
   - Source: `automation/downloads/`
   - Destination: `s3://airline-dashboard-project-data/automation/form41/`

2. **Stock Data**:
   - Source: `automation/data/airline_stock_data.csv`
   - Destination: `s3://airline-dashboard-project-data/automation/stockdata/`

---

### Step 7: Set Up AWS Glue Jobs
Set up two AWS Glue ETL jobs using the provided scripts:
1. **Form 41 Data Processing**:
   - Script: `automation/AWS Glue ETL Job for Form 41 Data Processing.py`

2. **Data Cleaning and Aggregation**:
   - Script: `automation/AWS Glue ETL Job for DataCleanAggregate.py`

Run the jobs in sequence:
1. Execute the Form 41 Data Processing job.
2. After completion, run the Data Cleaning and Aggregation job.

The cleaned and aggregated data will be available at:
```
s3://airline-dashboard-project-data/automation/merged_airline_data/
```

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

---
