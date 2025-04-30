import os
import logging
from datetime import datetime
import json
import pandas as pd
import openpyxl
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--file", help="JSON file to process")
parser.add_argument("-t", "--test_case", help="Test case to run (e.g., tc_04_DomainValue_Check)")
args = parser.parse_args()

dt=datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
log_file=f"C:\\Users\\suren\\PycharmProjects\\ETL Automation_CITI\\Logs\\Testing_log_{dt}.log"
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt="%d-%m-%Y %H-%M-%S", force=True)

TestResultPath=f"C:\\Users\\suren\\PycharmProjects\\ETL Automation_CITI\\TestResults\\DomainCheckResult_{dt}.xlsx"
config_path= r'C:\Users\suren\PycharmProjects\ETL Automation_CITI\ConfigFiles_Scenarios'

def test_DomainCheck(Oracle_Conn):
    all_Table_result = []
    if args.file:
        each_json_file = os.path.join(config_path, args.file)
        logging.info(f"** Processing file {each_json_file} **")
        try:
            with open(each_json_file, 'r') as SQL_file:
                SQL_Queries = json.load(SQL_file)
            if args.test_case and args.test_case in SQL_Queries:
                if args.test_case == "tc_04_DomainValue Check":
                    t_table = SQL_Queries["Source and Target Tables"]["t_table"]
                    logging.info(f"Target Table name: {t_table}")
                    column_defs = SQL_Queries[args.test_case]["columns"]
                    where = SQL_Queries[args.test_case]["where_con"]
                    logging.info(f"Columns are {column_defs}")
                    logging.info(f"where clause {where}")
                    cursor = Oracle_Conn.cursor()
                    for column, settings in column_defs.items():
                        logging.info(f"Column in for loop {column}")
                        allowed_values = settings["allowed_values"]
                        query = f"SELECT DISTINCT {column} FROM {t_table} "
                        cursor.execute(query)
                        values = [row[0] for row in cursor.fetchall()]
                        logging.info(f"Values are {values}")
                        invalid_values = [val for val in values if val not in allowed_values]
                        result = {
                            "Table": t_table,
                            "Column": column,
                            "Found Values": str(values),
                            "Allowed Values": str(allowed_values),
                            "Invalid Values": str(invalid_values) if invalid_values else "",
                            "Status": "FAIL" if invalid_values else "PASS",
                            "Error": ""
                        }
                        all_Table_result.append(result)
                        logging.info(f"Column: {column}, Found values: {values}, Allowed: {allowed_values}, Invalid: {invalid_values}")
                else:
                    logging.info(f"Test case {args.test_case} is not supported for dynamic execution.")
            else:
                logging.info(f"Test case {args.test_case} not found in the JSON file.")
        except Exception as e:
            logging.error(f"in file - {args.file}: {e}")
    else:
        for filename in os.listdir(config_path):
            each_json_file = config_path + '\\' + filename
            logging.info("** Started Domain Value Check Validation **")
            logging.info(f"** Processing file {each_json_file} **")
            try:
                with open(each_json_file, 'r') as SQL_file:
                    SQL_Queries = json.load(SQL_file)
                t_table = SQL_Queries["Source and Target Tables"]["t_table"]
                logging.info(f"Target Table name: {t_table}")
                column_defs = SQL_Queries["tc_04_DomainValue Check"]["columns"]
                where=SQL_Queries["tc_04_DomainValue Check"]["where_con"]
                logging.info(f"Columns are {column_defs}")
                logging.info(f"where clause {where}")
                cursor = Oracle_Conn.cursor()
                for column, settings in column_defs.items():
                    logging.info(f"Column in for loop {column}")
                    allowed_values = settings["allowed_values"]
                    query = f"SELECT DISTINCT {column} FROM {t_table} "
                    cursor.execute(query)
                    values = [row[0] for row in cursor.fetchall()]
                    logging.info(f"Values are {values}")
                    invalid_values = [val for val in values if val not in allowed_values]
                    result = {
                        "Table": t_table,
                        "Column": column,
                        "Found Values": str(values),
                        "Allowed Values": str(allowed_values),
                        "Invalid Values": str(invalid_values),
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
    df = pd.DataFrame(all_Table_result)
    df.to_excel(TestResultPath, index=False, sheet_name='Domain_Validation_Results')
    logging.info(f"Domain validation results written to: {TestResultPath}")

    # Fail the test if any column failed
    failed_columns = [r for r in all_Table_result if r["Status"] == "FAIL"]
    assert not failed_columns, f"Some columns failed domain validation. See {TestResultPath}"
