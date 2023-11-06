"""
Class for transforming CSV files to desired format
"""
import pandas as pd
from abc import abstractmethod


class CSVTransformer:
    """
    This base class reads a CSV file into a DataFrame and transforms it based
    on the trasform() method which should be extended since it is dataset-specific.
    """
    def __init__(self) -> None:
        """
        Instantiates a CSVTransformer object.

        Parameters
        ----------
        None

        Returns
        ----------
        None
        """
        pass

    def get_raw_data(self, fp: str) -> pd.DataFrame:
        """
        Reads raw CSV file into DataFrame.

        Parameters
        ----------
        fp: str - Filepath of raw CSV file

        Returns
        ----------
        DataFrame - Dataframe for further processing
        """
        return pd.read_csv(fp)
    
    def write_data(self, df: pd.DataFrame, fp: str) -> None:
        """
        Writes transformed data to a CSV file.

        Parameters
        ----------
        df: pd.DataFrame - Transformed dataframe
        fp: str - Output filepath for transformed data

        Returns
        ----------
        None
        """
        df.to_csv(fp, index=False)
    
    @abstractmethod
    def transform(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw data to desired form.

        Parameters
        ----------
        df: pd.DataFrame - Raw dataframe

        Returns
        ----------
        pd.DataFrame - Transformed dataframe
        """
    

class ShortSalesTranformer(CSVTransformer):
    """
    Extension from CSVTransformer to transform Daily Short Sale Balances
    """
    def transform(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        # Delete the last row
        df = raw_df.drop(raw_df.index[-1])

        # Extract the first two rows as column level information
        new_columns_index = df.iloc[:2].values.tolist()

        # Remove the first two rows from the DataFrame
        df = df.iloc[2:]

        # Set the extracted rows as the new multi-index for columns
        df.columns = pd.MultiIndex.from_arrays(new_columns_index)

        # Use the first level as the prefix for the second level
        df.columns = df.columns.map(
            lambda x: f'Margin_{x[1]}' if x[0] == 'Margin Short Sales' else (
                f'SBL_{x[1]}' if x[0] == 'SBL Short Sales' else x[1]
            )
        )

        # Convert columns to correct data type
        for col in df:
            if col not in ['Security Code', 'Note']:
                df[col] = df[col].astype(float)

        # Calculate Short Volume Pct
        change_in_day_balance = (df['SBL_Current Day Balance']-df['SBL_Previous Day Balance']) \
                            + (df['Margin_Current Day Balance']-df['Margin_Previous Day Balance'])
        total_quota = df['Margin_Quota']+df['SBL_Quota for the Next Day']
        df['Short Volume Pct'] = 100 * (change_in_day_balance / total_quota)

        # Group Avg Short Volume Pct & Group Std Short Volume Pct
        df['Group'] = df['Security Code'].str[0]
        group_stats = df.groupby('Group')['Short Volume Pct'].agg(['mean', 'std'])
        df = df.merge(group_stats, left_on='Group', right_index=True, suffixes=('', '_Group'))
        df.rename(columns={'mean': 'Group Avg Short Volume Pct', 'std': 'Group Std Short Volume Pct'}, inplace=True)

        # Calculate the Z-scores based on group average and group standard deviation
        df['Group Z score Short Volume Pct'] = (df['Short Volume Pct'] - df['Group Avg Short Volume Pct']) / df['Group Std Short Volume Pct']

        # Filter companies with Z-score >= 2 or NaN
        filtered_df = df[(df['Group Z score Short Volume Pct'] >= 2)]

        final_df = filtered_df[[
            'Security Code', 
            'Short Volume Pct', 
            'Group Avg Short Volume Pct', 
            'Group Std Short Volume Pct', 
            'Group Z score Short Volume Pct'
        ]]

        return final_df
