import pandas as pd
import json
from datetime import datetime
import os
import logging

dt=datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
log_file=f"C:\\Users\\suren\\PycharmProjects\\ETLAutomation with pytest\\Logs\\ETL_TEsting_logs_{dt}.log"
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt="%d-%m-%Y %H-%M-%S", force=True)

def Source_Target_Count_check(MySQL_Conn,Oracle_Conn):
    logging.info("*** Source and Target Tables validation initiated ***")
    logging.info("*** MySQL Connection initiated ***")
    logging.info("*** Oracle Database connection initiated ***")

    Config_testscenarios_dir=r'C:\Users\suren\PycharmProjects\ETLAutomation with pytest\config_TestScenarios'

    all_table_result_list=[]

    #Loop through all JSON Files in directory
    for filename in os.listdir(Config_testscenarios_dir):
        ts_json_file_path=Config_testscenarios_dir +'\\' + filename

        try:

            logging.info(f"* Initiated processing of file:{filename} *")
            # Load SQL queries from all JSON files into python program
            with open(ts_json_file_path, 'r') as ts_SQL_file:
                SQL_Queries = json.load(ts_SQL_file)
            logging.info(f"SQueries are {SQL_Queries}")
            # Capturing Source & Target table names from .json file
            s_table_name = SQL_Queries["tc_01_Source & Target Tables"]["s_table"]
            t_table_name = SQL_Queries["tc_01_Source & Target Tables"]["t_table"]
            logging.info(f"* Source Table:{s_table_name}  Target Table:{t_table_name} validations initiated *")

            # Run the source count query and load the results into a DataFrame
            s_query = SQL_Queries['tc_02_Verifying Source & Target Record counts']['s_count']
            source_cursor = MySQL_Conn.cursor()
            source_cursor.execute(s_query)
            df_source_cnt = pd.DataFrame(source_cursor)

            # Run the target count query and load the results into a DataFrame
            t_query = SQL_Queries['tc_02_Verifying Source & Target Record counts']['t_count']
            target_cursor = Oracle_Conn.cursor()
            target_cursor.execute(t_query)
            df_target_cnt = pd.DataFrame(target_cursor)

            if df_source_cnt.iloc[0, 0] == df_target_cnt.iloc[0, 0]:
                Result = "Source & Target counts are matched"
                status = "PASS"
            else:
                Result = "Source & Target counts are not matched"
                status = "FAIL"

            logging.info(
                f"* Source Table Count:{df_source_cnt.iloc[0, 0]}  Target Table Count:{df_target_cnt.iloc[0, 0]} *")

            # Create a DataFrame to write the result in Excel sheet
            df_s_t_count_result = pd.DataFrame(
                {
                    "Database": ["Source_MySql", "Target_Oracle"],
                    "Table_names": [s_table_name, t_table_name],
                    "Count": [df_source_cnt.iloc[0, 0], df_target_cnt.iloc[0, 0]],
                    "Result": [Result, None],
                    "Status": [status, None]
                }
            )

            # Add each result to the list

            all_table_result_list.append(df_s_t_count_result)

        except Exception as e:
            logging.error(f"in file - {filename}: {e}")
            continue

            # return all_table_result_list
    df_count_results = pd.concat(all_table_result_list, ignore_index=True)

    return df_count_results
