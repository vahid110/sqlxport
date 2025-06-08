# sqlxport/formats/base.py

from abc import ABC, abstractmethod
import pandas as pd


class FormatWriter(ABC):
    @abstractmethod
    def write(self, df: pd.DataFrame, file_path: str) -> str:
        """Write to a single file. Returns output path."""
        pass

    @abstractmethod
    def write_partitioned(self, df: pd.DataFrame, output_dir: str, partition_by: str = None) -> str:
        """Write partitioned output. Returns output directory."""
        pass

    @abstractmethod
    def write_flat(self, df: pd.DataFrame, output_dir: str) -> str:
        """Write non-partitioned multi-file output. Returns output directory."""
        pass
