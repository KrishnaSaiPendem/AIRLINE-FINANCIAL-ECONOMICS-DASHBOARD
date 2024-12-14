# Airline Data and Stock Analysis Project

This repository contains Python scripts and instructions for setting up an environment to pull and analyze airline data and stock market trends. The project utilizes tools like `selenium`, `yfinance`, and `pandas` for data extraction and processing.

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

---

## Setup Instructions

### Step 1: Clone the Repository
Clone this repository to your EC2 instance:
```bash
git clone https://github.com/your-username/airline-data-analysis.git
cd airline-data-analysis
