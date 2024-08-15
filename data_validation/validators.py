import json
import os
import numpy as np
import pandas as pd
import postcodes_uk
import pyarrow

from re import sub
from decimal import Decimal


class DataFrameObject:

    def __init__(self, df= None):
        self.df = df if df is not None else pd.DataFrame()
    
    def load_csv(self, filepath):
        '''Loads CSV file into dataframe object'''
        try:
            self.df = pd.read_csv(filepath)
        except:
            raise NotImplementedError("File not loaded")
    
    def check_null_values(self):
        '''Checks for null values and drops them'''
        if self.df.isnull().values.any() == True:
            try:
                self.df = self.df.dropna()
            except:
                raise NotImplementedError("Unable to drop null values")
        
        else: self.df = self.df
    
    def save_parquet(self, filepath):
        '''Saves dataframe as parquet file'''
        try:
            self.df.to_parquet(filepath)
        except:
            raise NotImplementedError("Unable to save dataframe")

   

class DataValidation:
    @staticmethod
    def check_int_column(df, column_number):
        """
        Validates that a specified column in the DataFrame contains integer values.
        Converts values to integers where possible and filters out rows with non-integer values.
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            column_number (int): The index of the column to check.
        
        Returns:
            pd.DataFrame: A DataFrame containing only rows with valid integers in the specified column.
        """
        
        def fun(x):
            try:
                return pd.to_numeric(x).astype(int)
            except:
                return 'error'

        try:
            df['int_valid'] = df.iloc[:, column_number].apply(lambda x: fun(x))

            if df['int_valid'].str.contains('error').any() == True:
                valid_df = df[~(df['int_valid'] == 'error')].drop('int_valid', axis = 1).reset_index(drop = True)
            
            else:
                response = "valid"
                valid_df = df.drop('int_valid', axis = 1).reset_index(drop = True)
        
        except:
            raise NotImplementedError("Unable to check column")
        
        return valid_df
    

    @staticmethod
    def check_currency_column(df, column_number):
        """
        Validates that a specified column in the DataFrame contains valid currency values.
        Strips non-numeric characters and converts values to Decimal, filtering out invalid entries.
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            column_number (int): The index of the column to check.
        
        Returns:
            pd.DataFrame: A DataFrame containing only rows with valid currency values.
        """
    
    # Defining a function to create decimal values
        def fun(x):
            try:
                return Decimal(sub(r'[^\d.]', '', str(x)))
            except:
                return 'error'
        
        
        try:
            df['numeric_currency'] = df.iloc[:, column_number].apply(lambda x: fun(x))
            
            if df['numeric_currency'].str.contains('error').any() == True:
                valid_df = df[~(df['numeric_currency'] == 'error')].drop('numeric_currency', axis = 1).reset_index(drop = True)
            
            else:
                valid_df = df.drop('numeric_currency', axis = 1).reset_index(drop = True)
        
        except:
            raise NotImplementedError("Unable to check column")
        
        return valid_df


    @staticmethod
    def check_date_column(df, column_number):
        """
        Validates that a specified column in the DataFrame contains valid date values.
        Filters out rows with invalid dates.
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            column_number (int): The index of the column to check.
        
        Returns:
            pd.DataFrame: A DataFrame containing only rows with valid dates.
        """  
    
        try:
            if pd.to_datetime(df.iloc[:, column_number],errors='coerce').isna().any() == True:
                df['validator'] = pd.to_datetime(df.iloc[:, column_number],errors='coerce')
                valid_df = df[~df['validator'].isna()].drop('validator', axis = 1).reset_index(drop = True)
            
            else:
                valid_df = df
        
        except:
            raise NotImplementedError("Unable to check column")

        return valid_df


    @staticmethod
    def check_postcode_column(df, column_number):
        """
        Validates that a specified column in the DataFrame contains valid UK postcodes.
        Filters out rows with invalid postcodes.
        
        Args:
            df (pd.DataFrame): The input DataFrame.
            column_number (int): The index of the column to check.
        
        Returns:
            pd.DataFrame: A DataFrame containing only rows with valid UK postcodes.
        """

        try:
            df['validated'] = df.iloc[:, column_number].apply(lambda x: postcodes_uk.validate(x))
            valid_df = df[df['validated'] == True].drop('validated', axis = 1).reset_index(drop = True)
        

        except:
            raise NotImplementedError("Unable to check column")
        
        return valid_df