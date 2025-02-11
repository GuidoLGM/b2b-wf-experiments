import pandas as pd

class DataEvaluationPreprocessor:

    REQUIRED_COLUMNS = ['Appointment_Day', 'SWT', 'Product_Grp', 'Work_Order_Action_Grp', 'District', 'Region_Type']
    DATA_RANGE = pd.to_datetime([
            "2024-07-01", "2024-08-01", "2024-09-01",
            "2024-10-01", "2024-11-01", "2024-12-01"
        ])

    
    def __init__(self, data: pd.DataFrame) -> None:

        self.data = data.copy()

        self.__check_columns()
        self.__sort_columns()
        self.__ensure_all_months()
        self.__remove_lower_priority_tiers()


        self.data['Appointment_Day'] = pd.to_datetime(self.data['Appointment_Day'])

    def __sort_columns(self):
        self.data = self.data[self.data.columns.sort_values()]
        
    def get_filtered_data(self, filters: dict):
        """
        Apply a set of filters to a dataframe.
        Expected format for filters is a dict, e.g. {'Region_Type': 'Tier 1'}.
        """
        filtered_data = self.data.copy()
        for col, val in filters.items():
            filtered_data = filtered_data[filtered_data[col] == val]
        return filtered_data
    
    def get_grouped_data(self, group_by: list[str] = None, filters: dict = None):

        if group_by is None:
            group_by = self.data.columns.drop(['SWT', 'Appointment_Day'])

        grouped_data = self.data.copy()
        
        if filters is not None:
            grouped_data = self.get_filtered_data(filters)
            
        grouped_data['series_id'] = grouped_data[group_by].astype(str).agg(' '.join, axis=1)
        grouped_data = grouped_data[['Appointment_Day', 'series_id','SWT']]
        grouped_data.set_index('Appointment_Day', inplace=True)
        grouped_data = grouped_data.sort_values(by=['Appointment_Day', 'series_id'])
        grouped_data = grouped_data.groupby(['Appointment_Day', 'series_id'], as_index=True)['SWT'].sum()
        return grouped_data

    def __remove_lower_priority_tiers(self) -> pd.DataFrame:
        """
        Among rows that match exactly on all columns except 'Region_Type',
        keep only the row with the smallest numeric tier (e.g. Tier 1 < Tier 2 < Tier 3).
        """
        tier_map = {"Tier 1": 1, "Tier 2": 2, "Tier 3": 3, "Tier 4": 4}
        df = self.data.copy()
        df["tier_rank"] = df["Region_Type"].map(tier_map)
        
        # 2) Sort by all columns that define a duplicate plus the numeric tier rank.
        #    For duplicates, the smallest tier_rank will come first in sort order.
        sort_cols = [c for c in df.columns if c not in ["Region_Type", "tier_rank"]]
        df.sort_values(by=sort_cols + ["tier_rank"], inplace=True, ignore_index=True)
        
        # 3) Drop duplicates on every column **except** "Region_Type" (and our temporary "tier_rank")
        #    This means we only keep the first row of each group (the smallest tier).
        dedup_cols = [c for c in df.columns if c not in ["Region_Type", "tier_rank"]]
        df.drop_duplicates(subset=dedup_cols, keep="first", inplace=True, ignore_index=True)
        
        # 4) Drop the helper column
        df.drop(columns=["tier_rank"], inplace=True)
        
        self.data = df.copy()
        return df       

    def __check_columns(self):
        """
            Check if the df has this columns:
            - Appointment_Day [date]
            - SWT [float]
            - Product_Grp [str]
            - Work_Order_Action_Grp [str]
            - District [str]
            - Region_Type [str]
        """
        
        missing_columns = set(self.REQUIRED_COLUMNS) - set(self.data.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")  

        # cast the data for the right types
        self.data['Appointment_Day'] = pd.to_datetime(self.data['Appointment_Day'])
        self.data['SWT'] = self.data['SWT'].astype(float)
        self.data['Product_Grp'] = self.data['Product_Grp'].astype(str)
        self.data['Work_Order_Action_Grp'] = self.data['Work_Order_Action_Grp'].astype(str)
        self.data['District'] = self.data['District'].astype(str)
        self.data['Region_Type'] = self.data['Region_Type'].astype(str)
            
    def __has_all_months(self):
        """
        Check if the df has all months from 2024-07-01 to 2024-12-01 for each series_id
        """
        data = self.data.copy()
        data['series_id'] = data[data.columns.drop(['SWT', 'Appointment_Day'])].astype(str).agg(' '.join, axis=1)
        series_ids = data['series_id'].unique()
        for series_id in series_ids:
            series_data = data[data['series_id'] == series_id]
            if len(series_data) != len(self.DATA_RANGE):
                return False
        return True

    def __ensure_all_months(self):
        """
        For every unique combination of columns (other than Appointment_Day and SWT),
        this function adds rows for all months between 2024-07-01 and 2024-12-01
        (inclusive) if they're missing. The SWT column is filled with 0 where data 
        does not exist in the original dataframe.
        """

        if self.__has_all_months():
            return self.data
        
        df = self.data.copy()
        df['Appointment_Day'] = pd.to_datetime(df['Appointment_Day'])

        id_cols = [col for col in df.columns if col not in ['Appointment_Day', 'SWT']]
        unique_ids = df[id_cols].drop_duplicates()
        months_df = pd.DataFrame({'Appointment_Day': self.DATA_RANGE})

        unique_ids['merge_key'] = 1
        months_df['merge_key'] = 1
        cross_joined = pd.merge(unique_ids, months_df, on='merge_key').drop(columns='merge_key')

        merged = pd.merge(
            cross_joined,
            df,
            on=id_cols + ['Appointment_Day'],
            how='left'
        )

        merged['SWT'] = merged['SWT'].fillna(0)

        merged = merged.sort_values(by=['Appointment_Day'] + id_cols).reset_index(drop=True)

        merged = merged[['Appointment_Day'] + sorted(set(merged.columns) - {'Appointment_Day', 'SWT'}) + ['SWT']]

        self.data = merged.copy()
        return merged