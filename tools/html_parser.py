"""
Class to parse HTML text
"""
import pandas as pd

class HTMLTableParser:
    """
    The HTMLTableParser parses, transforms, and saves table from raw HTML text as a 
    specified file format.
    """
    def __init__(self, target_filetype: str='csv') -> None:
        """
        Instantiates an HTMLTableParser object.

        Parameters
        ----------
        target_filetype: str - Filetype to write table as. Default = csv

        Returns
        ----------
        None
        """
        self.target_filetype = target_filetype

    def parse_table_from_raw_text(self, text: str) -> pd.DataFrame:
        """
        Parses table from HTML page and saves as DataFrame.

        Parameters
        ----------
        text: str - HTML text to parse

        Returns
        ----------
        pd.DataFrame - Parsed table
        """
        return pd.read_html(text)[0]

    def write_table(self, fp: str, df: pd.DataFrame) -> None:
        """
        Writes DataFrame as specified filetype to local machine.

        Parameters
        ----------
        fp: str - Filepath to write table
        df: DataFrame - DataFrame to write

        Returns
        ----------
        None
        """
        if self.target_filetype == 'csv':
            df.to_csv(fp, index=False)
        else:
            raise Exception(f'HTMLTableParser does not support {self.target_filetype} file format yet.')
