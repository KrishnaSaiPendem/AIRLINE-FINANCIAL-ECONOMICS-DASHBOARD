# Airline Data and Stock Data Download
This part of the repository contains Python, shell scripts and instructions for setting up an environment to extract, transform and load airline data and stock market trends data in Amazon S3 bucket for use in Machine Learning model building and dashboard. The project utilizes tools like `selenium`, `yfinance`, and `pandas` for data extraction and processing.

## Table of Contents
- [Automation Overview](#project-overview)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
  - [Step 1: Clone the Repository](#step-1-clone-the-repository)
  - [Step 2: Update System Packages](#step-2-update-system-packages)
  - [Step 3: Install Required Tools](#step-3-install-required-tools)
  - [Step 4: Install Python Packages](#step-4-install-python-packages)
  - [Step 5: Set Up the Project Directory](#step-5-set-up-the-project-directory)
  - [Step 6: Write and Run Scripts](#step-6-write-and-run-scripts)
  - [Step 7: Save and Analyze Data](#step-7-save-and-analyze-data)
- [Directory Structure](#directory-structure)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Automation Overview
The goal of this part is to:
1. Pull airline-related data from various sources.
2. Retrieve stock data for analysis using Python.
3. Save the processed data for further analysis.

The project is designed to run on an Amazon EC2 instance.

## Prerequisites
Before you begin, ensure you have the following:
- An Amazon EC2 instance (Amazon Linux 2023).
- Python 3.9 or higher installed on the instance.
- Basic knowledge of Python and Linux commands.
- AWS S3 buckets set up to copy files

---

## Setup Instructions

### Step 1: Clone the Repository
Clone this repository to your EC2 instance:
```bash
git clone https://github.com/your-username/airline-data-analysis.git
cd airline-data-analysis
```
Following files show be downloaded
```bash
airline-data-analysis/
├── automation/
│   ├── btsdatapull.py
│   ├── stockdatapull.py
│   ├── s3_upload.sh
```
### Step 2: Update System Packages
Update the system packages on your EC2 instance to ensure you have the latest versions:
```bash
sudo dnf update -y
```
### Step 3: Install Required Tools
Install the necessary tools for Python development and browser automation:
1. Install pip
```bash
sudo yum install python3-pip -y
```
2. Install Google Chrome on EC2
```
sudo wget https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm
sudo yum install ./google-chrome-stable_current_x86_64.rpm -y
```

### Step 4: Install Python Package
Install the required Python packages using pip:
```bash
pip3 install selenium webdriver-manager yfinance==0.2.44 pandas boto3
```
### Step 5: Run the scripts to dowload the files
Clone this repository to your EC2 instance:
```bash
python3 btsdatapull.py
python3 stockdatapull.py
```

Upon succesful completion the folder structure show be similar to below, where the downloaded data are in downloads and data folders.
```bash
airline-data-analysis/
├── automation/
│   ├── btsdatapull.py
│   ├── stockdatapull.py
│   ├── s3_upload.sh
|   ├───data
│   |   ├── airline_stock_data.csv
│   |   ├── stock_data_collection.log
|   ├───downloads
│   |   ├── T_F41SCHEDULE_B1.csv
│   |   ├── T_F41SCHEDULE_B11.csv
│   |   ├── T_F41SCHEDULE_B43.csv
│   |   ├── T_F41SCHEDULE_P10.csv
│   |   ├── T_F41SCHEDULE_P11.csv
│   |   ├── T_F41SCHEDULE_P12.csv
│   |   ├── T_F41SCHEDULE_P12A.csv
│   |   ├── T_F41SCHEDULE_P1A_EMP.csv
│   |   ├── T_F41SCHEDULE_P51.csv
│   |   ├── T_F41SCHEDULE_P52.csv
│   |   ├── T_F41SCHEDULE_P6.csv
│   |   ├── T_F41SCHEDULE_P7.csv
│   |   ├── T_SCHEDULE_T1.csv
│   |   ├── T_SCHEDULE_T2.csv
```
### Step 6: Copy Downloaded Files from EC2 to S3 Bucket.
Run the below command to execute the shell script to copy downloaded files form EC2 to S3 buckets
```bash
bash s3_upload.sh
```
Upon successful completion, the downloaded files should be copied to the S3 bucket named `airline-dashboard-project-data`, as defined in the `BUCKET` variable.

### Local Source Folders
1. **Form 41 Data**:
   - Source: `/home/ec2-user/automation/downloads/`
   - Destination: `s3://airline-dashboard-project-data/automation/form41/`

2. **Stock Data**:
   - Source: `/home/ec2-user/automation/data/airline_stock_data.csv`
   - Destination: `s3://airline-dashboard-project-data/automation/stockdata/`

### Step 7: Set Up AWS Glue Jobs to Clean and Aggregate Data
