import numpy as np
import pandas as pd

def model(dbt, session):
    # Get the raw data using dbt's ref function - fixed syntax
    bulk_df = dbt.ref("mrt_inventory_ageing_bulk_raw")

    df = bulk_df.to_pandas()
    
    # Initialize LV_DIFF columns
    for i in range(1, 8):
        df[f'LV_DIFF{i}'] = 0

    def process_dataframe(df):
        for idx, row in df.iterrows():
            # Initialize difference dictionary
            lv_diffs = {}

            # Step 1: Calculate lv_diff7
            lv_diffs['lv_diff7'] = row['QTY_7'] - row['N_QTY_7']

            # Step 2: Use N_QTY_6 to reduce lv_diff7
            n_qty_6 = row['N_QTY_6']
            if n_qty_6 != 0:
                if n_qty_6 >= lv_diffs['lv_diff7']:
                    n_qty_6 -= lv_diffs['lv_diff7']
                    lv_diffs['lv_diff7'] = 0
                else:
                    lv_diffs['lv_diff7'] -= n_qty_6
                    n_qty_6 = 0
                df.at[idx, 'N_QTY_6'] = n_qty_6

            lv_diffs['lv_diff6'] = row['QTY_6'] - df.at[idx, 'N_QTY_6']

            # Step 3: N_QTY_5
            n_qty_5 = row.get('N_QTY_5', 0)
            for key in ['lv_diff7', 'lv_diff6']:
                if lv_diffs[key] != 0:
                    if n_qty_5 >= lv_diffs[key]:
                        n_qty_5 -= lv_diffs[key]
                        lv_diffs[key] = 0
                    else:
                        lv_diffs[key] -= n_qty_5
                        n_qty_5 = 0
            df.at[idx, 'N_QTY_5'] = n_qty_5
            lv_diffs['lv_diff5'] = row.get('QTY_5', 0) - df.at[idx, 'N_QTY_5']

            # Step 4: N_QTY_4
            n_qty_4 = row.get('N_QTY_4', 0)
            for key in ['lv_diff7', 'lv_diff6', 'lv_diff5']:
                if lv_diffs[key] != 0:
                    if n_qty_4 >= lv_diffs[key]:
                        n_qty_4 -= lv_diffs[key]
                        lv_diffs[key] = 0
                    else:
                        lv_diffs[key] -= n_qty_4
                        n_qty_4 = 0
            df.at[idx, 'N_QTY_4'] = n_qty_4
            lv_diffs['lv_diff4'] = row.get('QTY_4', 0) - df.at[idx, 'N_QTY_4']

            # Step 5: N_QTY_3
            n_qty_3 = row.get('N_QTY_3', 0)
            for key in ['lv_diff7', 'lv_diff6', 'lv_diff5', 'lv_diff4']:
                if lv_diffs[key] != 0:
                    if n_qty_3 >= lv_diffs[key]:
                        n_qty_3 -= lv_diffs[key]
                        lv_diffs[key] = 0
                    else:
                        lv_diffs[key] -= n_qty_3
                        n_qty_3 = 0
            df.at[idx, 'N_QTY_3'] = n_qty_3
            lv_diffs['lv_diff3'] = row.get('QTY_3', 0) - df.at[idx, 'N_QTY_3']

            # Step 6: N_QTY_2
            n_qty_2 = row.get('N_QTY_2', 0)
            for key in ['lv_diff7', 'lv_diff6', 'lv_diff5', 'lv_diff4', 'lv_diff3']:
                if lv_diffs[key] != 0:
                    if n_qty_2 >= lv_diffs[key]:
                        n_qty_2 -= lv_diffs[key]
                        lv_diffs[key] = 0
                    else:
                        lv_diffs[key] -= n_qty_2
                        n_qty_2 = 0
            df.at[idx, 'N_QTY_2'] = n_qty_2
            lv_diffs['lv_diff2'] = row.get('QTY_2', 0) - df.at[idx, 'N_QTY_2']

            # Step 7: N_QTY_1
            n_qty_1 = row.get('N_QTY_1', 0)
            for key in ['lv_diff7', 'lv_diff6', 'lv_diff5', 'lv_diff4', 'lv_diff3', 'lv_diff2']:
                if lv_diffs[key] != 0:
                    if n_qty_1 >= lv_diffs[key]:
                        n_qty_1 -= lv_diffs[key]
                        lv_diffs[key] = 0
                    else:
                        lv_diffs[key] -= n_qty_1
                        n_qty_1 = 0
            df.at[idx, 'N_QTY_1'] = n_qty_1
            lv_diffs['lv_diff1'] = row.get('QTY_1', 0) - df.at[idx, 'N_QTY_1']

            # Final: Write LV_DIFF* back to dataframe
            for i in range(1, 8):
                df.at[idx, f'LV_DIFF{i}'] = round(lv_diffs[f'lv_diff{i}'], 2)

        return df

    def multiply_lv_diff_values(df):
        for x in range(1, 8):  # X from 1 to 7
            col_name = f'LV_DIFF{x}'
            if col_name in df.columns and 'VAL' in df.columns:  # Changed 'val' to 'VAL'
                df[f'{col_name}_VALUE'] = df[col_name] * df['VAL']  # Changed value to VALUE
        return df

    def sum_lv_diff_and_values(df):
        # Sum LV_DIFF1 to LV_DIFF7
        diff_cols = [f'LV_DIFF{x}' for x in range(1, 8) if f'LV_DIFF{x}' in df.columns]
        df['LV_DIFF_total'] = df[diff_cols].sum(axis=1)

        # Sum LV_DIFF1_VALUE to LV_DIFF7_VALUE
        value_cols = [f'LV_DIFF{x}_VALUE' for x in range(1, 8) if f'LV_DIFF{x}_VALUE' in df.columns]
        df['LV_DIFF_VALUE_total'] = df[value_cols].sum(axis=1)

        return df

    # Process the data
    df = process_dataframe(df)
    df = multiply_lv_diff_values(df)
    df = sum_lv_diff_and_values(df)

    # Round float columns - only process columns that exist in the DataFrame
    float_cols = ['VAL', 'QTY_1', 'QTY_2', 'QTY_3', 'QTY_4', 'QTY_5',
        'QTY_6', 'QTY_7', 'N_QTY_1', 'N_QTY_2', 'N_QTY_3', 'N_QTY_4', 'N_QTY_5',
        'N_QTY_6', 'N_QTY_7', 'TOTAL_QTY', 'LV_DIFF1', 'LV_DIFF2', 'LV_DIFF3',
        'LV_DIFF4', 'LV_DIFF5', 'LV_DIFF6', 'LV_DIFF7',
        'LV_DIFF1_VALUE', 'LV_DIFF2_VALUE', 'LV_DIFF3_VALUE', 'LV_DIFF4_VALUE', 
        'LV_DIFF5_VALUE', 'LV_DIFF6_VALUE', 'LV_DIFF7_VALUE',
        'LV_DIFF_total', 'LV_DIFF_VALUE_total']
    
    # Only process columns that exist in the DataFrame
    existing_float_cols = [col for col in float_cols if col in df.columns]
    for col in existing_float_cols:
        df[col] = np.ceil(df[col] * 100) / 100

    # Sort data for consistency
    df = df.sort_values(by=['MATERIAL_ID', 'PLANT_ID', 'STORAGE_LOCATION', 'BATCH_NUMBER'], ascending=True)

    required_cols = ['MATERIAL_ID', 'MATERIAL_DESCRIPTION', 'PLANT_ID', 'PLANT_NAME',
        'BASE_UNIT_OF_MEASURE', 'UNIT', 'STORAGE_LOCATION', 'BATCH_NUMBER',
        'LV_DIFF_total', 'LV_DIFF_VALUE_total',
        'LV_DIFF1', 'LV_DIFF1_VALUE', 'LV_DIFF2', 'LV_DIFF2_VALUE',
        'LV_DIFF3', 'LV_DIFF3_VALUE', 'LV_DIFF4', 'LV_DIFF4_VALUE', 
        'LV_DIFF5', 'LV_DIFF5_VALUE', 'LV_DIFF6', 'LV_DIFF6_VALUE', 
        'LV_DIFF7', 'LV_DIFF7_VALUE']

    # Only select columns that exist in the DataFrame
    existing_required_cols = [col for col in required_cols if col in df.columns]
    grouped_df = df[existing_required_cols]
    grouped_df = grouped_df[grouped_df['LV_DIFF_total'] != 0]

    renamed_col = {
        'LV_DIFF1': 'DAYS_0_TO_30_QUANTITY',
        'LV_DIFF1_VALUE': 'DAYS_0_TO_30',
        'LV_DIFF2': 'DAYS_31_TO_60_QUANTITY',
        'LV_DIFF2_VALUE': 'DAYS_31_TO_60',
        'LV_DIFF3': 'DAYS_61_TO_90_QUANTITY',
        'LV_DIFF3_VALUE': 'DAYS_61_TO_90',
        'LV_DIFF4': 'DAYS_91_TO_120_QUANTITY',
        'LV_DIFF4_VALUE': 'DAYS_91_TO_120',
        'LV_DIFF5': 'DAYS_121_TO_180_QUANTITY',
        'LV_DIFF5_VALUE': 'DAYS_121_TO_180',
        'LV_DIFF6': 'DAYS_181_TO_365_QUANTITY',
        'LV_DIFF6_VALUE': 'DAYS_181_TO_365',
        'LV_DIFF7': 'OVER_1_YEAR_QUANTITY',
        'LV_DIFF7_VALUE': 'OVER_1_YEAR_VALUE',
        'LV_DIFF_total': 'STOCK_QTY',
        'LV_DIFF_VALUE_total': 'STOCK_VALUE'
    }

    grouped_df = grouped_df.rename(columns=renamed_col)
    grouped_df.columns = grouped_df.columns.str.upper()

    # (grouped_df.head(1))


    return grouped_df 