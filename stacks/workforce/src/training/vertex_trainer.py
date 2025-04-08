from typing import Optional, List, Dict, Any
from google.cloud import aiplatform
from datetime import datetime
from abc import ABC, abstractmethod

class BaseTrainer(ABC):
    """Abstract base class for all training implementations."""
    
    @abstractmethod
    def train(self, **kwargs) -> aiplatform.Model:
        """Train the model and return the trained model."""
        pass

class AutoMLTrainer(BaseTrainer):
    """Implementation of AutoML training on Vertex AI."""
    
    def __init__(
        self,
        experiment_name: str,
        dataset_name: str,
        model_name: str,
        column_specs: Dict[str, str],
        optimization_objective: str,
        attribute_columns: List[str],
        holiday_regions: Optional[List[str]] = None
    ):
        self.experiment_name = experiment_name
        self.dataset_name = dataset_name
        self.model_name = model_name
        self.column_specs = column_specs
        self.optimization_objective = optimization_objective
        self.attribute_columns = attribute_columns
        self.holiday_regions = holiday_regions or []
        
        # Initialize experiment tracking
        self.experiment = self._get_or_create_experiment()
        
    def _get_or_create_experiment(self) -> aiplatform.Experiment:
        """Get existing experiment or create a new one."""
        experiment_list = aiplatform.Experiment.list(
            filter=f"display_name={self.experiment_name}"
        )
        
        if len(experiment_list) == 0:
            return aiplatform.Experiment.create(
                display_name=self.experiment_name,
                description=f"Workforce prediction experiment created on {datetime.now().isoformat()}"
            )
        return experiment_list[0]
    
    def _get_or_create_dataset(self, bq_source: str) -> aiplatform.TimeSeriesDataset:
        """Get existing dataset or create a new one."""
        dataset_list = aiplatform.TimeSeriesDataset.list(
            filter=f"display_name={self.dataset_name}"
        )
        
        if len(dataset_list) == 0:
            print("... creating new dataset ... ")
            return aiplatform.TimeSeriesDataset.create(
                display_name=self.dataset_name,
                bq_source=[bq_source],
            )
        
        print("... using existent dataset ... ")
        return dataset_list[0]
    
    def _get_parent_model(self) -> Optional[str]:
        """Get parent model if exists."""
        model_list = aiplatform.Model.list(
            filter=f"display_name={self.model_name}"
        )
        
        if len(model_list) == 0:
            print("... training a new model ... ")
            return None
            
        print("... using existent model ... ")
        model = model_list[0]
        print(model)
        return model.resource_name
    
    def train(
        self,
        bq_source: str,
        target_column: str,
        time_column: str,
        time_series_identifier_column: str,
        forecast_horizon: int,
        context_window: int,
        data_granularity_unit: str,
        **kwargs
    ) -> aiplatform.Model:
        """
        Train an AutoML forecasting model on Vertex AI.
        
        Args:
            bq_source: BigQuery source path for the training data
            target_column: Name of the target column to predict
            time_column: Name of the timestamp column
            time_series_identifier_column: Column that identifies each time series
            forecast_horizon: Number of time steps to forecast
            context_window: Number of time steps to use as context
            data_granularity_unit: Time unit for the data (e.g., "hour", "day")
            **kwargs: Additional arguments passed to the training job
        """
        
        # Start experiment run
        with self.experiment.run(self.model_name) as experiment_run:
            # Log parameters
            experiment_run.log_params({
                "target_column": target_column,
                "time_column": time_column,
                "forecast_horizon": forecast_horizon,
                "context_window": context_window,
                "data_granularity_unit": data_granularity_unit,
                "optimization_objective": self.optimization_objective
            })
            
            # Get or create dataset
            dataset = self._get_or_create_dataset(bq_source)
            
            # Get parent model if exists
            parent_model = self._get_parent_model()
            
            # Initialize and run training job
            training_job = aiplatform.AutoMLForecastingTrainingJob(
                display_name=self.model_name,
                optimization_objective=self.optimization_objective,
                column_specs=self.column_specs,
            )
            
            model = training_job.run(
                dataset=dataset,
                target_column=target_column,
                time_column=time_column,
                time_series_identifier_column=time_series_identifier_column,
                available_at_forecast_columns=[time_column],
                unavailable_at_forecast_columns=[target_column],
                time_series_attribute_columns=self.attribute_columns,
                forecast_horizon=forecast_horizon,
                context_window=context_window,
                data_granularity_unit=data_granularity_unit,
                data_granularity_count=1,
                weight_column=None,
                budget_milli_node_hours=1000,
                parent_model=parent_model,
                model_display_name=self.model_name,
                is_default_version=True,
                model_version_description=f"{self.experiment_name} model generated on {datetime.now().date().isoformat()}",
                predefined_split_column_name=None,
                holiday_regions=self.holiday_regions
            )
            
            # Log metrics from the trained model
            metrics = model.get_model_evaluation().metrics
            experiment_run.log_metrics(metrics)
            
            return model

class CustomModelTrainer(BaseTrainer):
    """Base class for custom model implementations (SARIMA, Prophet, etc)."""
    
    def __init__(self, experiment_name: str):
        self.experiment_name = experiment_name
        self.experiment = self._get_or_create_experiment()
    
    def _get_or_create_experiment(self) -> aiplatform.Experiment:
        """Get existing experiment or create a new one."""
        experiment_list = aiplatform.Experiment.list(
            filter=f"display_name={self.experiment_name}"
        )
        
        if len(experiment_list) == 0:
            return aiplatform.Experiment.create(
                display_name=self.experiment_name,
                description=f"Workforce prediction experiment created on {datetime.now().isoformat()}"
            )
        return experiment_list[0]
    
    @abstractmethod
    def train(self, **kwargs) -> aiplatform.Model:
        """
        Train a custom model.
        
        This method should be implemented by specific model classes
        (e.g., SARIMATrainer, ProphetTrainer) to handle their unique
        training requirements while maintaining experiment tracking.
        """
        pass

# Example implementation of a custom model trainer
class ProphetTrainer(CustomModelTrainer):
    """Example implementation for Prophet model training."""
    
    def train(self, **kwargs) -> aiplatform.Model:
        with self.experiment.run("prophet_training") as experiment_run:
            # Log parameters
            experiment_run.log_params(kwargs)
            
            # Implement Prophet-specific training logic here
            # This is just a placeholder - actual implementation would
            # include Prophet model training and conversion to Vertex AI model
            raise NotImplementedError("Prophet training not yet implemented")
