import pickle
from google.cloud import aiplatform
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from kfp.dsl import component, Input, Output, Artifact, Model, Dataset

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def generate_rolling_predictions(values, window_size, horizon):
    """Generate predictions using a rolling window approach"""
    predictions = []
    rolling_window = np.array(values[-window_size:])
    
    for _ in range(horizon):
        next_pred = np.mean(rolling_window)
        predictions.append(next_pred)
        rolling_window = np.roll(rolling_window, -1)
        rolling_window[-1] = next_pred
        
    return predictions

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/b2b_ai/wf_pipeline/training:1.0.2-rc"
)
def sma_trainer_component(
    dataset: Input[Dataset],
    experiment_name: str,
    window_size: int,
    project_id: str,
    project_location: str,
    time_column: str,
    target_column: str,
    series_identifier: str,
    forecast_horizon: int,
    run_name: str,
    output_model: Output[Model],
    experiment_run: Output[Artifact]
):
    logger.info(f"Starting SMA trainer component with window size: {window_size}")
    
    logger.info(f"Initializing Vertex AI SDK for project: {project_id}, location: {project_location}")
    aiplatform.init(project=project_id, location=project_location, experiment=experiment_name)

    logger.info(f"Reading dataset from: {dataset.path}")
    df = pd.read_csv(dataset.path, parse_dates=[time_column])
    logger.info(f"Dataset shape: {df.shape}")

    model_output = {
        'model_type': 'SMA',
        'parameters': {'window_size': window_size},
        'predictions': {}
    }

    timestamp = datetime.now().strftime('%Y-%m-%d-%H%M')
    run = aiplatform.start_run(f"{run_name}-{timestamp}")

    unique_series = df[series_identifier].unique()
    unique_splits = df['split_index'].unique()
    
    total_combinations = len(unique_series) * len(unique_splits)
    successful_predictions = 0
    
    for series_id in unique_series:
        for split_index in unique_splits:
            series_data = df[
                (df[series_identifier] == series_id) & 
                (df['split_index'] == split_index)
            ].sort_values(time_column)
            
            if len(series_data) == 0:
                logger.warning(f"No data for series {series_id}, split {split_index}")
                continue
            
            try:
                historical_values = series_data[target_column].values
                future_predictions = generate_rolling_predictions(
                    historical_values,
                    window_size,
                    forecast_horizon
                )
                
                last_date = series_data[time_column].iloc[-1]
                future_dates = [(last_date + timedelta(days=i+1)).strftime('%Y-%m-%d')
                              for i in range(forecast_horizon)]
                
                model_output['predictions'][(series_id, split_index)] = {
                    'series_id': series_id,
                    'split_index': split_index,
                    'timestamps': future_dates,
                    'values': future_predictions,
                    'metadata': {
                        'last_training_date': last_date.strftime('%Y-%m-%d'),
                        'window_size': window_size
                    }
                }
                successful_predictions += 1
            except Exception as e:
                logger.error(f"Error processing series {series_id}, split {split_index}: {str(e)}")
    
    logger.info(f"Successfully processed {successful_predictions} out of {total_combinations} combinations")
    
    with open(output_model.path, "wb") as f:
        pickle.dump(model_output, f)

    run.log_params({
        "window_size": window_size,
        "model_uri": output_model.path,
        "forecast_horizon": forecast_horizon,
        "total_series": len(unique_series),
        "total_splits": len(unique_splits)
    })
    
    run.log_metrics({
        "total_combinations": total_combinations,
        "successful_predictions": successful_predictions,
        "completion_rate": successful_predictions / total_combinations
    })

    run_info = {
        "run_name": run.name,
        "experiment": experiment_name,
        "project_id": project_id,
        "location": project_location
    }
    with open(experiment_run.path, 'w') as f:
        json.dump(run_info, f)

    logger.info("SMA trainer component completed successfully")

sma_trainer_component_job = create_custom_training_job_from_component(
    sma_trainer_component,
    display_name='sma-model-training',
    machine_type='e2-highcpu-16',
    service_account='notebook-service-account@wb-ai-acltr-tbs-3-pr-a62583.iam.gserviceaccount.com'
)
