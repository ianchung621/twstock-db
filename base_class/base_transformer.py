from abc import ABC, abstractmethod
import pandas as pd

class BaseTransformer(ABC):
    """
    Base class for transformers that operate on internal data and return a DataFrame.
    """

    @abstractmethod
    def run(self) -> pd.DataFrame:
        pass

class Transformer(BaseTransformer):
    """
    Transformer that runs without external inputs.
    """
    def __init__(self):
        pass