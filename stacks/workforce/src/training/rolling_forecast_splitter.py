from typing import Dict, List, NamedTuple
from kfp.v2.dsl import component, Input, Output, Dataset, Artifact

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/mlops/kfp-2.0.0/kfp-load-model-slim:1.0.0"
)
def split_rolling_forecast(
    input_dataset: Input[Dataset],
    time_column: str,
    output_datasets: Output[Artifact]
) -> NamedTuple('Outputs', [
    ('train_paths', List[str]),
    ('test_paths', List[str]),
    ('split_info', List[Dict[str, str]])
]):
    """
    Splits the preprocessed data into rolling forecast datasets for model training and evaluation.
    Creates 4 train-test splits for rolling forecast evaluation, where each split uses
    progressively more data for training and tests on the subsequent 3-month period.

    Args:
        input_dataset: Input path for the preprocessed CSV dataset containing the time series data
        time_column: Name of the timestamp column in the dataset
        output_datasets: Output artifact that will contain paths to all train/test datasets

    Returns:
        NamedTuple containing:
        - train_paths: List of paths to training datasets for each split
        - test_paths: List of paths to test datasets for each split
        - split_info: List of dictionaries containing date ranges for each split

    Dataset Structure:
        For each split i (1-4):
        - train_data_i.csv: Training data up to split date
        - test_data_i.csv: Test data for subsequent 3 months

    Split Periods:
        1. Train: 2022-01-01 to 2024-03-31, Test: 2024-04-01 to 2024-06-30
        2. Train: 2022-01-01 to 2024-06-30, Test: 2024-07-01 to 2024-09-30
        3. Train: 2022-01-01 to 2024-09-30, Test: 2024-10-01 to 2024-12-31
        4. Train: 2022-01-01 to 2024-12-31, Test: 2025-01-01 to 2025-03-31
    """
    
    import os
    import json
    import logging
    import pandas as pd
    from datetime import datetime
    from google.cloud import aiplatform
    
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info("Reading input dataset from: %s", input_dataset.path)
    forecast_processed_data = pd.read_csv(input_dataset.path, index_col=False)
    forecast_processed_data[time_column] = pd.to_datetime(forecast_processed_data[time_column])
    logger.info("Dataset loaded with shape: %s", forecast_processed_data.shape)
    
    splits = [
        {
            "train_start": "2022-01-01",
            "train_end":   "2024-03-31",
            "test_start":  "2024-04-01",
            "test_end":    "2024-06-30"
        },
        {
            "train_start": "2022-01-01",
            "train_end":   "2024-06-30",
            "test_start":  "2024-07-01",
            "test_end":    "2024-09-30"
        },
        {
            "train_start": "2022-01-01",
            "train_end":   "2024-09-30",
            "test_start":  "2024-10-01",
            "test_end":    "2024-12-31"
        },
        {
            "train_start": "2022-01-01",
            "train_end":   "2024-12-31",
            "test_start":  "2025-01-01",
            "test_end":    "2025-03-31"
        },
    ]
    
    # Convert dates to datetime
    for s in splits:
        for key in s:
            s[key] = pd.to_datetime(s[key])
    
    # Get staging bucket
    staging_bucket = aiplatform.initializer.global_config.staging_bucket
    
    # Initialize lists to store paths and metadata
    train_paths = []
    test_paths = []
    split_metadata = []
    
    logger.info("Starting data splitting process")
    for i, split in enumerate(splits, 1):
        logger.info(f"\nProcessing Split {i}:")
        logger.info(f"Training period: {split['train_start'].date()} to {split['train_end'].date()}")
        logger.info(f"Testing period: {split['test_start'].date()} to {split['test_end'].date()}")
        
        # Create train/test masks
        train_mask = (forecast_processed_data[time_column] >= split["train_start"]) \
                   & (forecast_processed_data[time_column] <= split["train_end"])
        
        test_mask = (forecast_processed_data[time_column] >= split["test_start"]) \
                  & (forecast_processed_data[time_column] <= split["test_end"])

        # Split data
        train_df = forecast_processed_data.loc[train_mask].copy()
        test_df = forecast_processed_data.loc[test_mask].copy()
        
        # Log split statistics
        logger.info(f"Training data shape: {train_df.shape}")
        logger.info(f"Test data shape: {test_df.shape}")
        logger.info(f"Training date range: {train_df[time_column].min()} to {train_df[time_column].max()}")
        logger.info(f"Test date range: {test_df[time_column].min()} to {test_df[time_column].max()}")
        
        # Define paths
        train_path = os.path.join(staging_bucket, f"train_data_{i}.csv")
        test_path = os.path.join(staging_bucket, f"test_data_{i}.csv")
        
        # Save datasets
        train_df.to_csv(train_path, index=False)
        test_df.to_csv(test_path, index=False)
        
        # Store paths and metadata
        train_paths.append(train_path)
        test_paths.append(test_path)
        split_metadata.append({
            "split_number": i,
            "train_start": split["train_start"].strftime("%Y-%m-%d"),
            "train_end": split["train_end"].strftime("%Y-%m-%d"),
            "test_start": split["test_start"].strftime("%Y-%m-%d"),
            "test_end": split["test_end"].strftime("%Y-%m-%d"),
            "train_path": train_path,
            "test_path": test_path,
            "train_shape": train_df.shape,
            "test_shape": test_df.shape
        })
    
    # Save metadata to artifact
    metadata_path = os.path.join(staging_bucket, "split_metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(split_metadata, f, indent=2)
    
    logger.info("\nAll splits completed successfully")
    logger.info(f"Split metadata saved to: {metadata_path}")
    
    # Save all paths to output artifact
    output_datasets.metadata = {
        "train_paths": train_paths,
        "test_paths": test_paths,
        "metadata_path": metadata_path
    }
    
    from collections import namedtuple
    output = namedtuple('Outputs', ['train_paths', 'test_paths', 'split_info'])
    return output(
        train_paths=train_paths,
        test_paths=test_paths,
        split_info=split_metadata
    )
