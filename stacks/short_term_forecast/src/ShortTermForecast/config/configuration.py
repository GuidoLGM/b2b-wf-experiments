from typing import List

from src.ShortTermForecast.constants import *
from src.ShortTermForecast.utils.common import read_yaml, create_directories

from src.ShortTermForecast.entity.config_entity import DataIngestionConfig
from src.ShortTermForecast.entity.config_entity import CrossValSplitConfig


class ConfigurationManager:
    def __init__(
        self,
        config_filepath = CONFIG_FILE_PATH,
        params_filepath = PARAMS_FILE_PATH
    ):
        
        if config_filepath is not None:
            self.config = read_yaml(config_filepath)
            create_directories([self.config.artifacts_root])
        
        if params_filepath is not None:
            self.params = read_yaml(params_filepath)


    def get_data_ingestion_dvc_config(self) -> DataIngestionConfig:
        """
        Returns a DataIngestionConfig configured for DVC/local runs.
        """

        general_configs = self.config.general_setup
        data_ingestion_config = self.config.data_ingestion

        create_directories([data_ingestion_config.root_dir])

        return DataIngestionConfig(
            root_dir=Path(data_ingestion_config.root_dir),
            local_file_name=data_ingestion_config.local_file_name,
            project_id=general_configs.project_id,
            project_location=general_configs.project_location,
            bq_dataset=data_ingestion_config.bq_dataset,
            bq_source_table=data_ingestion_config.bq_source_table,
            time_column=general_configs.time_column,
            target_column=general_configs.target_column,
            series_identifier=general_configs.series_identifier,
            attribute_columns=general_configs.attribute_columns
        )
    
    def get_data_ingestion_kfp_config(
            self,
            project_id: str,
            project_location: str,
            bq_dataset: str,
            bq_source_table: str,
            time_column: str,
            target_column: str,
            series_identifier: str,
            attribute_columns: List[str]
    ) -> DataIngestionConfig:
        """
        Returns a DataIngestionConfig configured for Kubeflow Pipeline runs.
        """

        return DataIngestionConfig(
            root_dir=None,
            local_file_name=None,
            project_id=project_id,
            project_location=project_location,
            bq_dataset=bq_dataset,
            bq_source_table=bq_source_table,
            time_column=time_column,
            target_column=target_column,
            series_identifier=series_identifier,
            attribute_columns=attribute_columns
        )        
        
    def get_cross_validation_split_dvc_config(self) -> CrossValSplitConfig:
        
        general_configs = self.config.general_setup
        data_ingestion_config = self.config.data_ingestion
        cross_validation_config = self.config.cross_validation

        create_directories([cross_validation_config.root_dir])

        return CrossValSplitConfig(
            root_dir=Path(cross_validation_config.root_dir),
            input_dataset=Path(data_ingestion_config.root_dir, data_ingestion_config.local_file_name),
            time_column=general_configs.target_column,
            forecast_horizon=general_configs.forecast_horizon,
            train_file_name=cross_validation_config.train_file_name,
            test_file_name=cross_validation_config.test_file_name
        )
    
    def get_cross_validation_split_kfp_config(
            self,
            input_dataset: str,
            time_column: str,
            forecast_horizon: int
    ) -> CrossValSplitConfig:

        return CrossValSplitConfig(
            root_dir=None,
            input_dataset=input_dataset,
            time_column=time_column,
            forecast_horizon=forecast_horizon,
            train_file_name=None,
            test_file_name=None
        )   