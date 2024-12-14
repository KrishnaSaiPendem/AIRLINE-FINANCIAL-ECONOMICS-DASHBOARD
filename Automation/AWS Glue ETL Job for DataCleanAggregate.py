import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when, sum as spark_sum, round as spark_round, mean as spark_mean, expr, ntile
from pyspark.sql.window import Window
import warnings
import boto3
from botocore.exceptions import ClientError

# Suppress warnings
warnings.filterwarnings("ignore", message="DataFrame constructor is internal*")

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define paths
input_path = "s3://airline-dashboard-project-data/automation/form41/"
input_path_stock = "s3://airline-dashboard-project-data/automation/stockdata/"
output_path = "s3://airline-dashboard-project-data/automation/merged_airline_data"

# Function to check S3 path
def check_s3_path(bucket_path):
    """
    Check if S3 path exists and is accessible
    Returns tuple (exists: bool, error_message: str)
    """
    try:
        # Parse S3 path
        s3_path = bucket_path.replace('s3://', '')
        bucket_name = s3_path.split('/')[0]
        prefix = '/'.join(s3_path.split('/')[1:])
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False, f"Bucket {bucket_name} does not exist"
            elif error_code == '403':
                return False, f"Access denied to bucket {bucket_name}"
            else:
                return False, f"Error accessing bucket {bucket_name}: {str(e)}"
        
        # Check if prefix has contents
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        if 'Contents' not in response:
            return False, f"No files found at {bucket_path}"
            
        return True, "Path exists and is accessible"
        
    except Exception as e:
        return False, f"Error checking S3 path: {str(e)}"

# Function to read data from S3
def read_s3_csv(path):
    """
    Read CSV file from S3 with error handling
    """
    # Check S3 path first
    exists, message = check_s3_path(path)
    if not exists:
        raise Exception(f"S3 path validation failed: {message}")
    
    try:
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [path]},
            format="csv",
            format_options={
                "withHeader": True,
                "separator": ","
            }
        )
        return dynamic_frame.toDF()
    except Exception as e:
        raise Exception(f"Failed to read data from {path}: {str(e)}")

# Define files to check
files_to_check = {
    'Balance Sheet': f"{input_path}T_F41SCHEDULE_B1.csv",
    'Income Statement': f"{input_path}T_F41SCHEDULE_P12.csv",
    'Operating Expenses (Objective)': f"{input_path}T_F41SCHEDULE_P6.csv",
    'Operating Expenses (Functional)': f"{input_path}T_F41SCHEDULE_P7.csv",
    'Fuel Data': f"{input_path}T_F41SCHEDULE_P12A.csv",
    'Employment Data': f"{input_path}T_F41SCHEDULE_P1A_EMP.csv",
    'Stock Data': f"{input_path_stock}airline_stock_data.csv",
    'T1 Data': f"{input_path}T_SCHEDULE_T1.csv",
    'T2 Data': f"{input_path}T_SCHEDULE_T2.csv"
}

# Check each file before reading
print("\nChecking individual data files...")
for file_desc, file_path in files_to_check.items():
    exists, message = check_s3_path(file_path)
    if not exists:
        raise Exception(f"Required file missing - {file_desc}: {message}")
    print(f"✓ Found {file_desc}")

# If all checks pass, proceed with reading the files
print("\nAll files verified. Reading data...")

# Read all data files
b1_df = read_s3_csv(files_to_check['Balance Sheet'])
p1_2_df = read_s3_csv(files_to_check['Income Statement'])
p6_df = read_s3_csv(files_to_check['Operating Expenses (Objective)'])
p7_df = read_s3_csv(files_to_check['Operating Expenses (Functional)'])
p12a_df = read_s3_csv(files_to_check['Fuel Data'])
p1a_df = read_s3_csv(files_to_check['Employment Data'])
stock_df = read_s3_csv(files_to_check['Stock Data'])
t1_df = read_s3_csv(files_to_check['T1 Data'])
t2_df = read_s3_csv(files_to_check['T2 Data'])

print("\nAll dataframes read successfully...")

# Filter for major US passenger airlines
us_carriers = [
    'AA',  # American Airlines
    'UA',  # United Airlines
    'DL',  # Delta Air Lines
    'WN',  # Southwest Airlines
    'B6',  # JetBlue Airways
    'AS',  # Alaska Airlines
    'NK',  # Spirit Airlines
    'F9',  # Frontier Airlines
    'G4',  # Allegiant Air
    'HA',  # Hawaiian Airlines
    'OO',  # SkyWest Airlines
    '8C', # Air Transport Services Group
    'SY',  # Sun Country Airlines
    'YV'   # Mesa Air Group
]

# Filter all dataframes for major US carriers
b1_df = b1_df.filter(col('UNIQUE_CARRIER').isin(us_carriers))
p1_2_df = p1_2_df.filter(col('UNIQUE_CARRIER').isin(us_carriers))
p6_df = p6_df.filter(col('UNIQUE_CARRIER').isin(us_carriers))
p7_df = p7_df.filter(col('UNIQUE_CARRIER').isin(us_carriers))
p12a_df = p12a_df.filter(col('UNIQUE_CARRIER').isin(us_carriers))
p1a_df = p1a_df.filter(col('UNIQUE_CARRIER').isin(us_carriers))
t1_df = t1_df.filter(col('UNIQUE_CARRIER').isin(us_carriers))
t2_df = t2_df.filter(col('UNIQUE_CARRIER').isin(us_carriers))

print("\nAll dataframes filtered for major US carriers successfully...")

# Process B1 data
columns_to_keep = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER',
    'CASH', 'SHORT_TERM_INV', 'ACCTS_RECEIVABLE', 'CURR_ASSETS',
    'FLIGHT_EQUIP', 'PROP_EQUIP_NET', 'ASSETS',
    'ACCTS_PAY_TRADE', 'CURR_LIABILITIES', 'LONG_TERM_DEBT',
    'SH_HLD_EQUITY'
]
b1_df = b1_df.select(columns_to_keep)

print("\nB1 data processed successfully...")
# Process P1.2 data
essential_columns = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER',
    'TRANS_REV_PAX', 'PROP_BAG', 'RES_CANCEL_FEES', 'OP_REVENUES',
    'FLYING_OPS', 'MAINTENANCE', 'PAX_SERVICE', 'OP_EXPENSES',
    'OP_PROFIT_LOSS', 'NET_INCOME'
]
p1_2_df = p1_2_df.select(essential_columns)

print("\nP1.2 data processed successfully...")

# Group P1.2 data
group_by_columns = ['AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER']
aggregate_columns = [
    'TRANS_REV_PAX', 'PROP_BAG', 'RES_CANCEL_FEES', 'OP_REVENUES',
    'FLYING_OPS', 'MAINTENANCE', 'PAX_SERVICE', 'OP_EXPENSES',
    'OP_PROFIT_LOSS', 'NET_INCOME'
]

p1_2_df = p1_2_df.groupBy(group_by_columns).agg(
    *[spark_sum(col).alias(col) for col in aggregate_columns]
).orderBy(group_by_columns)

print("\nP1.2 data grouped successfully...")

# Process P6 data
essential_columns = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER',
    'SALARIES_FLIGHT', 'SALARIES_MAINT', 'SALARIES_TRAFFIC', 'SALARIES',
    'BENEFITS_PERSONNEL', 'BENEFITS_PENSIONS', 'BENEFITS', 'SALARIES_BENEFITS',
    'AIRCRAFT_FUEL', 'MAINT_MATERIAL', 'LANDING_FEES', 'RENTALS',
    'INSURANCE', 'DEPRECIATION', 'AMORTIZATION', 'OP_EXPENSE'
]
p6_df = p6_df.select(essential_columns)

# Group P6 data
aggregate_columns = [col for col in essential_columns if col not in ['AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER']]
p6_df = p6_df.groupBy(group_by_columns).agg(
    *[spark_sum(col).alias(col) for col in aggregate_columns]
).orderBy(group_by_columns)

print("\nP6 data grouped successfully...")
# Process P12a data
essential_columns = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'CARRIER_NAME', 'YEAR', 'QUARTER', 'MONTH',
    'TDOMT_GALLONS', 'TINT_GALLONS', 'TOTAL_GALLONS',
    'TDOMT_COST', 'TINT_COST', 'TOTAL_COST',
    'SDOMT_GALLONS', 'SINT_GALLONS', 'TS_GALLONS',
    'SDOMT_COST', 'SINT_COST', 'TS_COST'
]
p12a_df = p12a_df.select(essential_columns)

# Aggregate P12a fuel data
sum_cols = [
    'TDOMT_GALLONS', 'TINT_GALLONS', 'TOTAL_GALLONS',
    'TDOMT_COST', 'TINT_COST', 'TOTAL_COST'
]

p12a_df_quarterly = p12a_df.groupBy(['UNIQUE_CARRIER', 'YEAR', 'QUARTER']).agg(
    *[spark_sum(col).alias(col) for col in sum_cols]
)

print("\nP12A data grouped successfully...")

# Process P7 data
essential_columns = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER',
    'AIR_OP_EXPENSE', 'FL_ATT_EXPENSE', 'FOOD_EXPENSE', 'PAX_SVC_EXPENSE',
    'LINE_SVC_EXPENSE', 'LANDING_FEES', 'AIR_SVC_EXPENSE',
    'TRAFFIC_EXP_PAX', 'RES_EXP_PAX', 'AD_EXP_PAX',
    'ADMIN_EXPENSE', 'DEPR_EXP_MAINT', 'MAINT_PROP_EQUIP',
    'TOTAL_OP_EXPENSE'
]
p7_df = p7_df.select(essential_columns)


# Group P7 data
group_by_columns = ['UNIQUE_CARRIER', 'YEAR', 'QUARTER']
aggregate_columns = [col for col in essential_columns if col not in ['AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER']]
p7_df = p7_df.groupBy(group_by_columns).agg(
    *[spark_sum(col).alias(col) for col in aggregate_columns]
).orderBy(group_by_columns)

# Fill missing ratios with carrier average
ratio_cols = [col for col in p7_df.columns if 'share' in col]
for col in ratio_cols:
    carrier_mean = p7_df.groupBy('UNIQUE_CARRIER').agg(spark_mean(col).alias('mean_val'))
    p7_df = p7_df.join(carrier_mean, 'UNIQUE_CARRIER', 'left')
    p7_df = p7_df.withColumn(col, when(col(col).isNull(), col('mean_val')).otherwise(col(col)))
    p7_df = p7_df.drop('mean_val')

print("\nP7 data processed successfully...")

# Process P1a data
essential_columns = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME',
    'YEAR', 'MONTH', 'EMPFULL', 'EMPPART', 'EMPTOTAL', 'EMPFTE'
]
p1a_df = p1a_df.select(essential_columns)

def monthly_to_quarterly(df):
    df = df.withColumn('QUARTER', expr('(CAST(MONTH AS INT) - 1) DIV 3 + 1'))
    avg_columns = ['EMPFULL', 'EMPPART', 'EMPTOTAL', 'EMPFTE']
    quarterly_df = df.groupBy([
        'AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER'
    ]).agg(
        *[spark_round(spark_mean(col)).alias(col) for col in avg_columns]
    ).orderBy('UNIQUE_CARRIER', 'YEAR', 'QUARTER')
    return quarterly_df

p1a_df = monthly_to_quarterly(p1a_df)

print("\nP1A data processed successfully...")

# Process T1 data
columns_to_keep = [
    'AIRLINE_ID', 'YEAR', 'QUARTER', 'MONTH', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME',
    'REV_PAX_ENP_110', 'REV_PAX_MILES_140', 'AVL_SEAT_MILES_320',
    'REV_ACRFT_MILES_FLOWN_410', 'REV_ACRFT_DEP_PERF_510',
    'REV_ACRFT_HRS_AIRBORNE_610', 'ACRFT_HRS_RAMPTORAMP_630'
]
t1_df = t1_df.select(columns_to_keep)
t1_df = t1_df.withColumn('QUARTER', expr('(CAST(MONTH AS INT) - 1) DIV 3 + 1'))

id_columns = [
    'AIRLINE_ID',
    'UNIQUE_CARRIER', 
    'UNIQUE_CARRIER_NAME',
    'YEAR',
    'QUARTER'
]

sum_columns = [
    'REV_PAX_ENP_110',
    'REV_PAX_MILES_140',
    'AVL_SEAT_MILES_320',
    'REV_ACRFT_MILES_FLOWN_410',
    'REV_ACRFT_DEP_PERF_510',
    'REV_ACRFT_HRS_AIRBORNE_610',
    'ACRFT_HRS_RAMPTORAMP_630'
]

t1_df = t1_df.groupBy(id_columns).agg(
    *[spark_sum(col).alias(col) for col in sum_columns]
).orderBy('UNIQUE_CARRIER', 'YEAR', 'QUARTER')

print("\nT1 data processed successfully...")

# Process T2 data
columns_to_keep = [
    'AIRLINE_ID', 'YEAR', 'QUARTER', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME',
    'AVL_SEAT_MILES_320', 'REV_PAX_MILES_140', 'REV_ACRFT_HRS_AIRBORNE_610',
    'ACRFT_HRS_RAMPTORAMP_630', 'AIR_DAYS_EQUIP_810', 'AIRCRAFT_FUELS_921'
]
t2_df = t2_df.select(columns_to_keep)

numeric_cols = [col for col in t2_df.columns if col not in ['AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER']]
t2_df = t2_df.groupBy(['UNIQUE_CARRIER', 'YEAR', 'QUARTER']).agg(
    *[spark_sum(col).alias(col) for col in numeric_cols]
).orderBy('UNIQUE_CARRIER', 'YEAR', 'QUARTER')

print("\nT2 data processed successfully...")

# Merge all dataframes
def add_prefix_to_columns(df, prefix, exclude_cols=['UNIQUE_CARRIER', 'YEAR', 'QUARTER']):
    for col_name in df.columns:
        if col_name not in exclude_cols:
            df = df.withColumnRenamed(col_name, f'{prefix}_{col_name}')
    return df

b1_df = add_prefix_to_columns(b1_df, 'B1')
p1_2_df = add_prefix_to_columns(p1_2_df, 'P1_2')
p6_df = add_prefix_to_columns(p6_df, 'P6')
p7_df = add_prefix_to_columns(p7_df, 'P7')
p12a_df_quarterly = add_prefix_to_columns(p12a_df_quarterly, 'P12A')
p1a_df = add_prefix_to_columns(p1a_df, 'P1A')
t1_df = add_prefix_to_columns(t1_df, 'T1')
t2_df = add_prefix_to_columns(t2_df, 'T2')
stock_df = add_prefix_to_columns(stock_df, 'STOCK')

print("\nAll dataframes prefixed successfully...")
# Merge all dataframes using Spark SQL join
merged_df = b1_df.join(p1_2_df, ['UNIQUE_CARRIER', 'YEAR', 'QUARTER'], 'left')
merged_df = merged_df.join(p6_df, ['UNIQUE_CARRIER', 'YEAR', 'QUARTER'], 'left')
merged_df = merged_df.join(p7_df, ['UNIQUE_CARRIER', 'YEAR', 'QUARTER'], 'left')
merged_df = merged_df.join(p12a_df_quarterly, ['UNIQUE_CARRIER', 'YEAR', 'QUARTER'], 'left')
merged_df = merged_df.join(p1a_df, ['UNIQUE_CARRIER', 'YEAR', 'QUARTER'], 'left')
merged_df = merged_df.join(t1_df, ['UNIQUE_CARRIER', 'YEAR', 'QUARTER'], 'left')
merged_df = merged_df.join(t2_df, ['UNIQUE_CARRIER', 'YEAR', 'QUARTER'], 'left')
merged_df = merged_df.join(stock_df, ['UNIQUE_CARRIER', 'YEAR', 'QUARTER'], 'left')

print("\nAll dataframes merged successfully...")

# Add carrier type columns
def transform(df):
    window_spec = Window.partitionBy('UNIQUE_CARRIER')
    df = df.withColumn('avg_revenue', spark_mean('P1_2_OP_REVENUES').over(window_spec))
    df = df.withColumn('avg_employees', spark_mean('P1A_EMPTOTAL').over(window_spec))
    
    revenue_window = Window.orderBy('avg_revenue')
    df = df.withColumn('airline_size', 
        when(ntile(3).over(revenue_window) == 1, 'Small')
        .when(ntile(3).over(revenue_window) == 2, 'Medium')
        .otherwise('Large'))
    
    df = df.withColumn('carrier_type', 
        when(col('UNIQUE_CARRIER').isin(['AA', 'UA', 'DL']), 'Legacy')
        .when(col('UNIQUE_CARRIER').isin(['WN', 'B6', 'NK', 'F9', 'G4', 'SY']), 'Low-Cost')
        .when(col('UNIQUE_CARRIER').isin(['OO', 'YV']), 'Regional')
        .when(col('UNIQUE_CARRIER').isin(['AS', 'HA']), 'Regional Legacy')
        .when(col('UNIQUE_CARRIER') == '8C', 'Cargo')
        .otherwise('Other'))
    
    return df

merged_df = transform(merged_df)

print("\nAll dataframes transformed successfully...")

# Export as CSV
output_dyf = DynamicFrame.fromDF(merged_df, glueContext, "output")
glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": []
    },
    format="csv", 
    format_options={
        "separator": ",",
        "writeHeader": True,
        "quoteChar": -1
    }
)

# Rename any files in output directory to ensure .csv extension
try:
    # Parse S3 path
    s3_path = output_path.replace('s3://', '')
    bucket_name = s3_path.split('/')[0]
    prefix = '/'.join(s3_path.split('/')[1:]).rsplit('/', 1)[0] + '/'
    
    s3_client = boto3.client('s3')
    
    # List all objects in the output directory
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            old_key = obj['Key']
            # Skip if file already ends with .csv
            if old_key.endswith('.csv'):
                continue
                
            # Create new key with .csv extension
            new_key = old_key + '.csv'
            
            # Copy object with new name
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': old_key},
                Key=new_key
            )
            
            # Delete old object
            s3_client.delete_object(Bucket=bucket_name, Key=old_key)
            
            print(f"Renamed {old_key} to {new_key}")
            
    print("File extension renaming complete")
except Exception as e:
    print(f"Error during file renaming: {str(e)}")

print(f"\nFinal dataframe exported successfully to:")
print(f"CSV: {output_path}")

# End the Glue job
job.commit()
