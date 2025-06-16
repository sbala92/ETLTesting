import pandas as pd
import json
from datetime import datetime
import os
import logging

dt=datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
log_file=f"C:\\Users\\suren\\PycharmProjects\\ETLAutomation with pytest\\Logs\\ETL_TEsting_logs_{dt}.log"
logging.basicConfig(filename=log_file, level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt="%d-%m-%Y %H-%M-%S", force=True)
def NullChecks(target_db):
    logging.info("*** ORacle Database connection initiated ***")
    logging.info("*** Null check initiated***")

    Config_testscenarios_dir = r'C:\Users\suren\PycharmProjects\ETLAutomation with pytest\config_TestScenarios'
    all_results = []

    logging.info("* Null checks initiated in Target Table *")

    # Loop through all JSON files in the directoryy
    for filename in os.listdir(Config_testscenarios_dir):
        tc_json_file_path = Config_testscenarios_dir + '/' + filename
        logging.info(f"Filenames: {tc_json_file_path}")

        try:
            # Load SQL queries from all JSON files into python program
            with open(tc_json_file_path, 'r') as SQL_file:
                SQL_Queries = json.load(SQL_file)

            # Fetch the target table name
            t_table_name = SQL_Queries["tc_01_Source & Target Tables"]["t_table"]

            logging.info(f"* Target Table:{t_table_name} validations initiated *")

            target_cursor = target_db.cursor()

            # Capturing null check queries for all target tables from all .Json files
            t_query = SQL_Queries['tc_03_Verifying Null values in Target table']['null_count']
            target_cursor.execute(t_query)
            df_null_cnt = pd.DataFrame(target_cursor)

            # Find if there are any null values
            if df_null_cnt.empty or df_null_cnt.iloc[0, 0] == 0:
                null_count = 0
                null_records = None  # No null rows
                result = "Null records not found!"
                status = "PASS"
            else:
                null_count = df_null_cnt.iloc[0, 0]
                null_rec_query = SQL_Queries['tc_03_Verifying Null Values in Target Table']['null_records']
                target_cursor.execute(null_rec_query)
                null_records = pd.DataFrame(target_cursor).to_string(index=False, header=False)
                null_records = ','.join(null_records.split())
                result = "Null records found!"
                status = "FAIL"
                logging.info(f"Null records found in table {t_table_name}: {null_records}")

            # Create a DataFrame to return the result
            df_nulls = pd.DataFrame(
                {
                    "Database": ["Target_Oracle"],
                    'Table_names': [t_table_name],
                    'Count': [null_count],
                    'Result': [result],
                    'Status': [status],
                    'Null_Records': [null_records]
                }
            )

            # Append the result for the current file to the list
            all_results.append(df_nulls)

        except Exception as e:
            logging.error(f"in file - {filename}: {e}")
            continue

    df_null_result = pd.concat(all_results, ignore_index=True)
    return df_null_result