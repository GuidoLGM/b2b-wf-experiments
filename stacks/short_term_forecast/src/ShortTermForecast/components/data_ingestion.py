from pathlib import Path
from google.cloud import bigquery

from src.ShortTermForecast import logger
from src.ShortTermForecast.entity.config_entity import DataIngestionConfig

class DataIngestion:
    """
    A class for ingesting data from BigQuery, processing it, and saving it locally.

    This class handles the creation of series identifiers, data loading from BigQuery,
    and saving the processed data to a local CSV file.

    Attributes:
        config (DataIngestionConfig): Configuration object containing necessary parameters.
        client (bigquery.Client): BigQuery client for data querying.
        data (pandas.DataFrame): Dataframe to store the ingested data.
    """

    def __init__(self, config: DataIngestionConfig):
        """
        Initialize the DataIngestion class.

        Args:
            config (DataIngestionConfig): Configuration object containing necessary parameters.
        """
        self.config = config
        self.client = None
        self.data = None
        logger.info("DataIngestion instance initialized with provided configuration.")

    def _create_series_identifier(self) -> str:
        """
        Create the series identifier query field based on the attribute columns.

        This method generates a SQL CONCAT statement to combine multiple attribute columns
        into a single series identifier.

        Example:
            attribute_columns = ['local', 'type']
            series identifier string = '<local> <type>' (e.g., 'vancouver 1')

        Returns:
            str: The CONCAT query to create the series identifiers column in the BigQuery view query.
        """
        coalesce_parts = [f"COALESCE({column}, 'None')" for column in self.config.attribute_columns]
        separator = "' '"
        series_identifier = f"CONCAT({f', {separator}, '.join(coalesce_parts)}) AS {self.config.series_identifier}"
        logger.debug(f"Created series identifier: {series_identifier}")
        return series_identifier

    def load(self):
        """
        Load data from BigQuery using the configured query.

        This method initializes the BigQuery client and executes the data ingestion query,
        storing the results in a pandas DataFrame.
        """
        logger.info("Initializing BigQuery client and loading data.")
        self.client = bigquery.Client(
            project=self.config.project_id,
            location=self.config.project_location
        )

        try:
            self.data = self.client.query(self.data_ingestion_query).to_dataframe()
            logger.info(f"Data loaded successfully. Shape: {self.data.shape}")
        except Exception as e:
            logger.error(f"Error loading data from BigQuery: {str(e)}")
            raise

    def save(self, save_path: str = None):
        """
        Save the loaded data to a CSV file.

        Args:
            save_path (str, optional): The path where the CSV file will be saved.
                If not provided, it will use the default path from the configuration.
        """
        if save_path is None:
            save_path = Path(self.config.root_dir, self.config.local_file_name)
        
        logger.info(f"Saving data to {save_path}")
        try:
            self.data.to_csv(save_path, index=False)
            logger.info("Data saved successfully.")
        except Exception as e:
            logger.error(f"Error saving data to CSV: {str(e)}")
            raise

    @property
    def data_ingestion_query(self) -> str:
        """
        Generate the BigQuery SQL query for data ingestion.

        This property creates a SQL query that selects and aggregates data from the source table,
        applies date filtering, and includes the series identifier.

        Returns:
            str: The complete SQL query string for data ingestion.
        """
        query = f"""
        WITH historical_table AS (
            SELECT 
                {self.config.time_column},
                {self.attribute_string},
                SUM({self.config.target_column}) AS {self.config.target_column}
            FROM 
                `{self.config.project_id}.{self.config.bq_dataset}.{self.config.bq_source_table}`
            WHERE 
                {self.config.time_column} <= DATE('2025-03-31')
            GROUP BY 
                {self.config.time_column},
                {self.attribute_string}
        )
        SELECT 
            {self._create_series_identifier()},
            {self.config.time_column},
            {self.attribute_string},
            {self.config.target_column}
        FROM historical_table
        """
        logger.debug("Generated data ingestion query.")
        return query

    @property
    def attribute_string(self) -> str:
        """
        Generate a comma-separated string of attribute columns.

        Returns:
            str: A comma-separated string of attribute column names.
        """
        return ','.join(self.config.attribute_columns)