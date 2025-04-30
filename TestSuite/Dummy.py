import logging
import sys
from datetime import datetime
import os
import json
import pandas as pd
import pytest
from pytest import fixture

# Set up logging
dt = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
log_dir = r"C:\Users\suren\PycharmProjects\ETL Automation_CITI\Logs"
log_file = os.path.join(log_dir, f"Test_Log_{dt}.log")

# Ensure log folder exists
os.makedirs(log_dir, exist_ok=True)

# Setup logging
logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%d-%m-%Y %H-%M-%S",
    force=True
)

logging.info("=== LOGGING TEST STARTED ===")
logging.info(f"Arguments received: {sys.argv}")

# Ensure arguments are passed correctly
if len(sys.argv) < 3:
    logging.error("No arguments passed. Exiting...")
    sys.exit(1)

# Get the filenames and columns from command line arguments
json_files = sys.argv[1:-1]  # All arguments except the last one are JSON files
columns_to_check = sys.argv[-1].split()  # The last argument is the list of columns

logging.info(f"Received arguments: {sys.argv[1:]}")

# Define path to your JSON files folder
config_path = r'C:\Users\suren\PycharmProjects\ETL Automation_CITI\ConfigFiles_Scenarios'

# Function to test Domain Check
@pytest.mark.usefixtures('Oracle_Conn')  # Use Oracle connection fixture from conftest.py
def test_DomainCheck(Oracle_Conn):
    all_Table_result = []

    for filename in json_files:
        # Combine the config folder path and the provided filename to form the full path
        each_json_file = os.path.join(config_path, filename)

        # Ensure the file exists
        if not os.path.exists(each_json_file):
            logging.error(f"JSON file {filename} not found at {config_path}")
            continue

        logging.info(f"** Processing file {each_json_file} **")

        try:
            # Open and load the JSON file
            with open(each_json_file, 'r') as SQL_file:
                SQL_Queries = json.load(SQL_file)

                # Extract table name and column definitions
                t_table = SQL_Queries["Source and Target Tables"]["t_table"]
                logging.info(f"Target Table name: {t_table}")

                column_defs = SQL_Queries["tc_04_DomainValue Check"]["columns"]
                where = SQL_Queries["tc_04_DomainValue Check"]["where_con"]
                logging.info(f"Columns are {column_defs}")
                logging.info(f"Where clause {where}")

                cursor = Oracle_Conn.cursor()
                for column in columns_to_check:
                    if column in column_defs:
                        logging.info(f"Column to check: {column}")
                        allowed_values = column_defs[column]["allowed_values"]
                        query = f"SELECT DISTINCT {column} FROM {t_table} " + where

                        cursor.execute(query)
                        values = [row[0] for row in cursor.fetchall()]

                        logging.info(f"Values fetched for column {column}: {values}")

                        # Check for invalid values
                        invalid_values = [val for val in values if val not in allowed_values]

                        # Prepare result
                        result = {
                            "Table": t_table,
                            "Column": column,
                            "Found Values": str(values),
                            "Allowed Values": str(allowed_values),
                            "Invalid Values": str(invalid_values) if invalid_values else "",
                            "Status": "FAIL" if invalid_values else "PASS",
                            "Error": ""
                        }

                        # Append result to the overall results
                        all_Table_result.append(result)
                        logging.info(f"Column: {column}, Found values: {values}, Allowed: {allowed_values}, Invalid: {invalid_values}")
                    else:
                        logging.warning(f"Column {column} not found in the JSON configuration.")

        except Exception as e:
            logging.error(f"In file - {filename}: {e}")

    # Write results to Excel
    TestResultPath = f"C:\\Users\\suren\\PycharmProjects\\ETL Automation_CITI\\TestResults\\DoaminCheckResult_{dt}.xlsx"
    df = pd.DataFrame(all_Table_result)
    df.to_excel(TestResultPath, index=False, sheet_name='Domain_Validation_Results')
    logging.info(f"Domain validation results written to: {TestResultPath}")

    # Fail the test if any column failed
    failed_columns = [r for r in all_Table_result if r["Status"] == "FAIL"]
    assert not failed_columns, f"Some columns failed domain validation. See {TestResultPath}"

    # Close the Oracle connection
    Oracle_Conn.close()
    logging.info("Oracle connection closed.")
