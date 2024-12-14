import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

# Initialize Glue context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'])

# Define common variables
allowed_carriers = [
    '8C', 'AA', 'AS', 'B6', 'DL', 'F9', 'G4', 'HA', 
    'NK', 'OO', 'SY', 'UA', 'WN', 'YV'
]

# Function to process each table
def process_table(
    input_path,
    output_path,
    columns_to_keep,
    filter_column='UNIQUE_CARRIER'
):
    # Read input data
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [input_path]},
        format="csv",
        format_options={
            "withHeader": True,
            "separator": ","
        }
    )
    
    # Convert to DataFrame for easier processing
    df = dynamic_frame.toDF()
    
    # Filter carriers
    df = df.filter(col(filter_column).isin(allowed_carriers))
    
    # Select specified columns
    df = df.select([col(c) for c in columns_to_keep])
    
    # Convert back to DynamicFrame
    output_frame = DynamicFrame.fromDF(df, glueContext, "output_frame")
    
    # Write output
    glueContext.write_dynamic_frame.from_options(
        frame=output_frame,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": []
        },
        format="csv",
        format_options={
            "separator": ",",
            "quoteChar": '"'
        }
    )

# Process Form B1
b1_columns = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER',
    'CASH', 'SHORT_TERM_INV', 'ACCTS_RECEIVABLE', 'CURR_ASSETS',
    'FLIGHT_EQUIP', 'PROP_EQUIP_NET', 'ASSETS',
    'ACCTS_PAY_TRADE', 'CURR_LIABILITIES', 'LONG_TERM_DEBT',
    'SH_HLD_EQUITY'
]
process_table(
    "s3://airline-dashboard-project-data/autodatapull/form41/T_F41SCHEDULE_B1.csv",
    "s3://airline-dashboard-project-data/autodatapull/filteredData/f41_b1_filtered.csv",
    b1_columns
)

# Process P12
p12_columns = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER',
    'TRANS_REV_PAX', 'PROP_BAG', 'RES_CANCEL_FEES', 'OP_REVENUES',
    'FLYING_OPS', 'MAINTENANCE', 'PAX_SERVICE', 'OP_EXPENSES',
    'OP_PROFIT_LOSS', 'NET_INCOME'
]
process_table(
    "s3://airline-dashboard-project-data/autodatapull/form41/T_F41SCHEDULE_P12.csv",
    "s3://airline-dashboard-project-data/autodatapull/filteredData/f41_p12_filtered.csv",
    p12_columns
)

# Process P6
p6_columns = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER',
    'SALARIES_FLIGHT', 'SALARIES_MAINT', 'SALARIES_TRAFFIC', 'SALARIES',
    'BENEFITS_PERSONNEL', 'BENEFITS_PENSIONS', 'BENEFITS', 'SALARIES_BENEFITS',
    'AIRCRAFT_FUEL', 'MAINT_MATERIAL', 'LANDING_FEES', 'RENTALS',
    'INSURANCE', 'DEPRECIATION', 'AMORTIZATION', 'OP_EXPENSE'
]
process_table(
    "s3://airline-dashboard-project-data/autodatapull/form41/T_F41SCHEDULE_P6.csv",
    "s3://airline-dashboard-project-data/autodatapull/filteredData/f41_p6_filtered.csv",
    p6_columns
)

# Process P12A
p12a_columns = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'CARRIER_NAME', 'YEAR', 'QUARTER', 'MONTH',
    'TDOMT_GALLONS', 'TINT_GALLONS', 'TOTAL_GALLONS',
    'TDOMT_COST', 'TINT_COST', 'TOTAL_COST',
    'SDOMT_GALLONS', 'SINT_GALLONS', 'TS_GALLONS',
    'SDOMT_COST', 'SINT_COST', 'TS_COST'
]
process_table(
    "s3://airline-dashboard-project-data/autodatapull/form41/T_F41SCHEDULE_P12A.csv",
    "s3://airline-dashboard-project-data/autodatapull/filteredData/f41_p12a_filtered.csv",
    p12a_columns
)

# Process P7
p7_columns = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 'YEAR', 'QUARTER',
    'AIR_OP_EXPENSE', 'FL_ATT_EXPENSE', 'FOOD_EXPENSE', 'PAX_SVC_EXPENSE',
    'LINE_SVC_EXPENSE', 'LANDING_FEES', 'AIR_SVC_EXPENSE',
    'TRAFFIC_EXP_PAX', 'RES_EXP_PAX', 'AD_EXP_PAX',
    'ADMIN_EXPENSE', 'DEPR_EXP_MAINT', 'MAINT_PROP_EQUIP',
    'TOTAL_OP_EXPENSE'
]
process_table(
    "s3://airline-dashboard-project-data/autodatapull/form41/T_F41SCHEDULE_P7.csv",
    "s3://airline-dashboard-project-data/autodatapull/filteredData/f41_p7_filtered.csv",
    p7_columns
)

# Process P1A
p1a_columns = [
    'AIRLINE_ID', 'UNIQUE_CARRIER', 'UNIQUE_CARRIER_NAME', 
    'YEAR', 'MONTH',
    'EMPFULL', 'EMPPART', 'EMPTOTAL', 'EMPFTE'
]
process_table(
    "s3://airline-dashboard-project-data/autodatapull/form41/T_F41SCHEDULE_P1A_EMP.csv",
    "s3://airline-dashboard-project-data/autodatapull/filteredData/f41_p1a_filtered.csv",
    p1a_columns
)

# Commit the job
job.commit()