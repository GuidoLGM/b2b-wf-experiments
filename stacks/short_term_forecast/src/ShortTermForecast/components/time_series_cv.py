import pandas as pd
from datetime import timedelta
from pathlib import Path

from src.ShortTermForecast import logger
from src.ShortTermForecast.entity.config_entity import CrossValSplitConfig

class TimeSeriesCV:
    """
    A class for implementing time series cross-validation with rolling forecast windows.
    
    This class handles the creation of training and test datasets using a rolling window
    cross-validation strategy for time series forecasting. It reads timestamp-based data
    and creates splits with an additional 'split_index' column to identify different
    temporal splits.
    
    Attributes:
        config (CrossValSplitConfig): Configuration object containing necessary parameters.
        data (pandas.DataFrame): DataFrame to store the input data.
        splits (list): List of dictionaries containing split configurations.
    """

    def __init__(self, config: CrossValSplitConfig) -> None:
        """
        Initialize the TimeSeriesCV class.

        Args:
            config (CrossValSplitConfig): Configuration object containing necessary parameters.
        """
        self.config = config
        self.data = None
        self.splits = None
        logger.info("TimeSeriesCV instance initialized with provided configuration.")

    def load(self):
        """
        Load data from the input dataset specified in the configuration.
        """
        logger.info(f"Reading input dataset from: {self.config.input_dataset}")
        try:
            self.data = pd.read_csv(self.config.input_dataset)
            # Ensure time column is properly converted to datetime
            self.data[self.config.time_column] = pd.to_datetime(self.data[self.config.time_column])
            logger.info(f"Data loaded successfully. Shape: {self.data.shape}")
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise

    def generate_splits(self):
        """
        Generate time series cross-validation splits.
        
        This method creates a list of split configurations, where each split represents
        a different forecasting period. All splits start training from 2022-01-01, with
        training periods growing progressively longer and test periods moving forward in time.
        """
        logger.info("Generating time series cross-validation splits")
        
        train_start = pd.to_datetime("2022-01-01")
        splits = []
        current_train_end = pd.to_datetime("2024-03-31")
        
        for split_index in range(1, 5):
            test_start = current_train_end + timedelta(days=1)
            test_end = test_start + timedelta(days=self.config.forecast_horizon-1)
            
            splits.append({
                "split_index": split_index,
                "train_start": train_start,
                "train_end": current_train_end,
                "test_start": test_start,
                "test_end": test_end
            })
            
            current_train_end = test_end
        
        self.splits = splits
        
        logger.info("Generated splits:")
        for s in splits:
            logger.info(f"Split {s['split_index']}:")
            logger.info(f"  Train: {s['train_start'].strftime('%Y-%m-%d')} to {s['train_end'].strftime('%Y-%m-%d')}")
            logger.info(f"  Test:  {s['test_start'].strftime('%Y-%m-%d')} to {s['test_end'].strftime('%Y-%m-%d')}")

    def process_splits(self):
        """
        Process the generated splits to create training and test datasets.
        
        This method applies the split configurations to the input data, creating
        separate training and test datasets with a 'split_index' column to identify
        different temporal splits.
        
        Returns:
            tuple: (combined_train, combined_test) DataFrames containing all splits
        """
        if self.splits is None:
            logger.error("Splits have not been generated. Call generate_splits() first.")
            raise ValueError("Splits not generated")
            
        all_train_data = []
        all_test_data = []
        
        for s in self.splits:
            logger.info(f"\nProcessing split {s['split_index']}")
            
            train_mask = (self.data[self.config.time_column] >= s["train_start"]) & \
                        (self.data[self.config.time_column] <= s["train_end"])
            
            test_mask = (self.data[self.config.time_column] >= s["test_start"]) & \
                       (self.data[self.config.time_column] <= s["test_end"])

            train_df = self.data.loc[train_mask].copy()
            test_df = self.data.loc[test_mask].copy()
            
            train_df['split_index'] = s['split_index']
            test_df['split_index'] = s['split_index']
            
            all_train_data.append(train_df)
            all_test_data.append(test_df)
            
            logger.info(f"Split {s['split_index']} - Train shape: {train_df.shape}, Test shape: {test_df.shape}")
        

        self.train = pd.concat(all_train_data, ignore_index=True)
        self.test = pd.concat(all_test_data, ignore_index=True)

    def save(self, save_train_path: str = None, save_test_path: str = None):
        """
        Save the processed training and test datasets to CSV files.

        Args:
            train_df (pd.DataFrame): Combined training dataset
            test_df (pd.DataFrame): Combined test dataset
        """

        if save_train_path is None:
            save_train_path = Path(self.config.root_dir, self.config.train_file_name)
        if save_test_path is None:
            save_test_path = Path(self.config.root_dir, self.config.test_file_name)
            
        logger.info(f"Saving combined training data (shape: {self.train.shape}) to {save_train_path}")
        self.train.to_csv(save_train_path, index=False)
        
        logger.info(f"Saving combined test data (shape: {self.test.shape}) to {save_test_path}")
        self.test.to_csv(save_test_path, index=False)
