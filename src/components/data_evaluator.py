import pandas as pd
import numpy as np

from components.data_evaluation_preprocessor import DataEvaluationPreprocessor

class Evaluation:
    
    def __init__(self, historical: DataEvaluationPreprocessor, predicted: DataEvaluationPreprocessor) -> None:
        """
        Initialize with two DataEvaluationPreprocessor objects: one for historical (actual) data
        and one for predicted (forecast) data.
        """
        self.historical = historical
        self.predicted = predicted

    def _get_data(
        self, 
        group_by: list[str] = None, 
        filters: dict = None
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Retrieve historical and predicted data (as DataFrames), optionally filtered and/or grouped.
        
        - If filters is provided (as a dict of {column: value}), the data are filtered.
        - If group is True, data are grouped (default grouping is by ['series_id', 'Appointment_Day'] 
          if no group_by is provided). In grouping, the SWT values are summed.
        """
        # Reset index to get Appointment_Month as a column.
        hist = self.historical.get_grouped_data(group_by, filters)
        pred = self.predicted.get_grouped_data(group_by, filters)

        return hist, pred

    def calculate_metric(
        self, 
        metric: str, 
        filters: dict = None, 
        group_by: list[str] = None, 
        epsilon: float = 1
    ) -> float:
        """
        Calculate a metric between the historical and predicted data.
        
        Parameters:
           metric:    str - Supported metrics: 'rmse', 'mape', or 'wape'
           group:     bool - Whether to group the raw data before merging.
                            If True, the data is grouped (default group_by = ['series_id', 'Appointment_Day']).
           filters:   dict - Optional filtering criteria (e.g. {'Region_Type': 'Tier 1'}).
           group_by:  list[str] - The columns to group by if group is True.
           epsilon:   float - A small value to avoid division by zero in percentage calculations.
        
        Returns:
           The computed metric as a float.
        """
        hist, pred = self._get_data(group_by, filters)
        
        merged = pd.merge(
            hist.reset_index(), pred.reset_index(), 
            on=['Appointment_Day', 'series_id'], 
            suffixes=('_hist', '_pred'), 
            how='inner'
        )
        
        error = merged['SWT_pred'] - merged['SWT_hist']
        
        if metric.lower() == 'rmse':
            return np.sqrt(np.mean(error ** 2))
        elif metric.lower() == 'mape':
            return np.mean(np.abs(error) / (merged['SWT_hist'] + epsilon)) * 100
        elif metric.lower() == 'wape':
            return np.sum(np.abs(error)) / (np.sum(merged['SWT_hist']) + epsilon)
        else:
            raise ValueError(f"Unsupported metric: {metric}")