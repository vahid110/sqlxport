# sqlxport/formats/base.py
from abc import ABC, abstractmethod
import pandas as pd

class FormatWriter(ABC):
    @abstractmethod
    def write(self, df: pd.DataFrame, file_path: str) -> None:
        pass

    @abstractmethod
    def write_partitioned(self, df: pd.DataFrame, output_dir: str, partition_by: str = None) -> None:
        pass
