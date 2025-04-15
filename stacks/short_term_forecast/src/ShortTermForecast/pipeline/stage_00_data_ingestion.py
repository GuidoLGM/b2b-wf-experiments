from src.ShortTermForecast import logger
from src.ShortTermForecast.components.data_ingestion import DataIngestion
from src.ShortTermForecast.config.configuration import ConfigurationManager

STAGE_NAME = "Data Ingestion"


class DataIngestionPipeline:
    def __init__(self):
        pass

    def main(self):
        config = ConfigurationManager()
        data_ingestion = DataIngestion(config.get_data_ingestion_dvc_config())
        data_ingestion.load()
        data_ingestion.save()

if __name__ == '__main__':
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
        obj = DataIngestionPipeline()
        obj.main()
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<")
        logger.info("\nx" + "=" * 50 + "x")
    except Exception as e:
        logger.exception(e)
        raise e