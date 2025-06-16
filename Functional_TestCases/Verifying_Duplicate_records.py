import pandas as pd
import json
import openpyxl
from datetime import datetime
import os
import logging

dt=datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
log_file=f"C:\\Users\\suren\\PycharmProjects\\ETLAutomation with pytest\\Logs\\ETL_TEsting_logs_{dt}.log"
logging.basicConfig(filename=log_file, level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt="%d-%m-%Y %H-%M-%S", force=True)
def Duplicate_records(target_db):
    logging.info("*** Oracle Database connection initiated ***")
    logging.info("*** Duplicate check initiated***")

    Config_testscenarios_dir = r'C:\Users\suren\PycharmProjects\ETLAutomation with pytest\config_TestScenarios'
    all_results = []

    logging.info("* Duplicate checks initiated in Target Table *")

    # Loop through all JSON files in the directory
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
            t_query=SQL_Queries["tc_04_Verifying duplicate records in Target table"]["dup_count"]
            target_cursor.execute(t_query)
            df_dup_cnt=pd.DataFrame(target_cursor)

            if df_dup_cnt.empty or df_dup_cnt.iloc[0,0]==0:
                dup_count=0
                dup_records=None
                result="No Duplicates Found"
                status='Pass'
            else:
                dup_count=df_dup_cnt.iloc[0,0]
                dup_rec_query=SQL_Queries["tc_04_Verifying duplicate records in Target table"]["dup_records"]
                target_cursor.execute(dup_rec_query)
                dup_records = pd.DataFrame(target_cursor).to_string(index=False, header=False)
                dup_records = ','.join(dup_records.split())
                result = "Duplicate rows found!"
                status = "FAIL"
                logging.info(f"Duplicate records found in table {t_table_name}: {dup_records}")

                # Create a DataFrame to write the result in Excel sheet
            df_Duplicate_records = pd.DataFrame(
                {
                    "Database": ["Target_Oracle"],
                    "Table_names": [t_table_name],
                    'Count': [dup_count],
                    'Result': [result],
                    'Status': [status],
                    'Duplicate_Records': [dup_records]
                }
            )

            # Append each result to the list
            all_results.append(df_Duplicate_records)



        except Exception as e:

            logging.error(f"in file - {filename}: {e}")

            continue

    df_duplicate_result = pd.concat(all_results, ignore_index=True)

    return df_duplicate_result

