from src.ShortTermForecast import logger
from src.ShortTermForecast.components.time_series_cv import TimeSeriesCV
from src.ShortTermForecast.config.configuration import ConfigurationManager

STAGE_NAME = "Cross Validation Split"


class TimeSeriesCVPipeline:
    def __init__(self):
        pass

    def main(self):
        config = ConfigurationManager()
        time_series_cv = TimeSeriesCV(config.get_cross_validation_split_dvc_config())
        time_series_cv.load()
        time_series_cv.generate_splits()
        time_series_cv.process_splits()
        time_series_cv.save()

if __name__ == '__main__':
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
        obj = TimeSeriesCVPipeline()
        obj.main()
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<")
        logger.info("\nx" + "=" * 50 + "x")
    except Exception as e:
        logger.exception(e)
        raise e