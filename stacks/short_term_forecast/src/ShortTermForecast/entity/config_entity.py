from dataclasses import dataclass
from pathlib import Path
from typing import List

@dataclass(frozen=True)
class DataIngestionConfig:
    root_dir: Path
    local_file_name: str
    project_id: str
    project_location: str
    bq_dataset: str
    bq_source_table: str
    time_column: str
    target_column: str
    series_identifier: str
    attribute_columns: List[str]
    
    
@dataclass(frozen=True)
class CrossValSplitConfig:
    root_dir: Path
    input_dataset: Path
    time_column: str
    forecast_horizon: int
    train_file_name: str
    test_file_name: str