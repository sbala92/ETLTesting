import pandas as pd
import json
import os
import logging
from datetime import datetime

dt = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
log_file=f"C:\\Users\\suren\\PycharmProjects\\ETLAutomation with pytest\\Logs\\ETL_TEsting_logs_{dt}.log"
logging.basicConfig(filename=log_file,level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt="%d-%m-%Y %H-%M-%S")
def Column_mapping_Validation(source_db,target_db):
    Config_testscenarios_dir = r'C:\Users\suren\PycharmProjects\ETLAutomation with pytest\config_TestScenarios'

    # List to collect results for all files
    all_table_result_list = []

    logging.info("*** Source & Target column mapping validation initiated ***")

    # Loop through all JSON files in the directory
    for filename in os.listdir(Config_testscenarios_dir):
        ts_json_file_path = Config_testscenarios_dir + '/' + filename

        try:
            logging.info(f"*** Initiated processing of file:{filename} ***")

            #Load SQL queries from all JSON files into python program
            with open(ts_json_file_path, 'r') as ts_SQL_file:
                SQL_Queries = json.load(ts_SQL_file)

            #Capturing Source & Target table names from .json file
            s_table_name = SQL_Queries["tc_01_Source & Target Tables"]["s_table"]
            t_table_name = SQL_Queries["tc_01_Source & Target Tables"]["t_table"]
            logging.info(f"*** Source Table:{s_table_name}  Target Table:{t_table_name} validations initiated ***")

            # Get the source and target queries for columns
            s_query = SQL_Queries['tc_05_Verifying Source & Target Columns mapping']['s_table']
            source_cursor = source_db.cursor()
            source_cursor.execute(s_query)
            df_source_result = pd.DataFrame(source_cursor)

            t_query = SQL_Queries['tc_05_Verifying Source & Target Columns mapping']['t_table']
            target_cursor = target_db.cursor()
            target_cursor.execute(t_query)
            df_target_result = pd.DataFrame(target_cursor)

            missing_records_in_target = []

            source_pids = df_source_result[0].tolist()
            target_pids = df_target_result[0].tolist()

            for pid in source_pids:
                if pid not in target_pids:
                    missing_records_in_target.append(pid)

            mismatch_source_target_records = []

            for i in range(len(df_target_result)):
                source_row = df_source_result.iloc[i].values
                target_row = df_target_result.iloc[i].values

                if any(source_row != target_row):
                        mismatch_source_target_records.append(source_row[0])

            if mismatch_source_target_records != [] or mismatch_source_target_records != [] :
                mismatch_result_df = pd.DataFrame(

                    {
                        "Database": ["Source table", "Target table"],
                        "Table_names": [s_table_name, t_table_name],
                        "Result": ["Source & Target tables data not matched!", None],
                        "Status": ["FAIL", None],
                        "Mismatched Source & Target Records": [mismatch_source_target_records, None],
                        "Missing Records in Target": [missing_records_in_target, None],


                    }
                    )
            else:
                mismatch_result_df = pd.DataFrame(
                            {
                                "Database": ["Source table", "Target table"],
                                "Table_names": [s_table_name, t_table_name],
                                "Result": ["Source & Target tables data matched!", None],
                                "Status": ["PASS", None],
                                "Mismatched Source & Target Records": [None, None],
                                "Missing Records in Target": [None, None],

                            }
                            )


                # Append the result for the current file to the list
            all_table_result_list.append(mismatch_result_df)

        except Exception as e:
            logging.error(f"in file - {filename}: {e}")
            continue

    df_mismatch_results = pd.concat(all_table_result_list, ignore_index=True)
    return df_mismatch_results