# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd

class LoaderInterface(ABC):
    """Interface for all data loaders"""
    
    @abstractmethod
    def load_dataframe(self, df: pd.DataFrame, destination_path: str, 
                      filename: Optional[str] = None, **kwargs) -> str:
        """Load DataFrame to destination"""
        pass
    
    @abstractmethod
    def delete_existing(self, path: str) -> bool:
        """Delete existing data at path"""
        pass
    
    @abstractmethod
    def list_files(self, path: str) -> List[str]:
        """List files at path"""
        pass
    
    @abstractmethod
    def path_exists(self, path: str) -> bool:
        """Check if path exists"""
        pass