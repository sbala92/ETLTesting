from datetime import datetime
import pandas as pd
from Functional_TestCases.Verifying_Source_Target_counts import Source_Target_Count_check
from Functional_TestCases.Verifying_Null_Values import NullChecks
from Functional_TestCases.Verifying_Duplicate_records import Duplicate_records
from Functional_TestCases.Verifying_Source_to_Target_Data_validation import Column_mapping_Validation

def test_tc_validations(MySQL_Conn,Oracle_Conn):
    dt = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    Test_Result_filepath = f"C:\\Users\\suren\\PycharmProjects\\ETLAutomation with pytest\\Testcase_Exec_result\\Test_Result_{dt}.xlsx"

    df_count_checks=Source_Target_Count_check(MySQL_Conn,Oracle_Conn)
    df_null_check=NullChecks(Oracle_Conn)
    df_dup_check=Duplicate_records(Oracle_Conn)
    df_col_mapp=Column_mapping_Validation(MySQL_Conn,Oracle_Conn)

    with pd.ExcelWriter(Test_Result_filepath) as Final_Result:
        df_count_checks.to_excel(Final_Result, sheet_name='Count_Result', index=False)
        df_null_check.to_excel(Final_Result, sheet_name='Null Check', index=False)
        df_dup_check.to_excel(Final_Result, sheet_name='Duplicate Check', index=False)
        df_col_mapp.to_excel(Final_Result, sheet_name='Column Mapping', index=False)



