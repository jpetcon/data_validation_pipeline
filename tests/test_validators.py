import pytest
import pandas as pd
from data_validation.validators import DataValidation

def test_check_int_column():
    # Sample DataFrame
    data = {'col1': ['1', '2', '3', 'a', '5']}
    df = pd.DataFrame(data)

    # Run validation
    valid_df = DataValidation.check_int_column(df, 0)

    # Assert the correct rows were filtered out
    expected_data = {'col1': ['1', '2', '3', '5']}
    expected_df = pd.DataFrame(expected_data)
    
    pd.testing.assert_frame_equal(valid_df, expected_df)


def test_check_currency_column():
    # Sample DataFrame
    data = {'col1': ['£1.99', '£2.99', '3.75', '1a.33', '£5.50']}
    df = pd.DataFrame(data)

    # Run validation
    valid_df = DataValidation.check_currency_column(df, 0)

    # Assert the correct rows were filtered out
    expected_data = {'col1': ['£1.99', '£2.99', '3.75', '£5.50']}
    expected_df = pd.DataFrame(expected_data)
    
    pd.testing.assert_frame_equal(valid_df, expected_df)


def test_check_date_column():
    # Sample DataFrame
    data = {'col1': ['2021-01-01', '1857-01-30', '1900-13-13', 'a']}
    df = pd.DataFrame(data)

    # Run validation
    valid_df = DataValidation.check_date_column(df, 0)

    # Assert the correct rows were filtered out
    expected_data = {'col1': ['2021-01-01', '1857-01-30', '1900-13-13', 'a']}
    expected_df = pd.DataFrame(expected_data)
    
    pd.testing.assert_frame_equal(valid_df, expected_df)


def test_check_postcode_column():
    # Sample DataFrame
    data = {'col1': ['SE1 1AA', 'PR2 9HT', '12345', 'a']}
    df = pd.DataFrame(data)

    # Run validation
    valid_df = DataValidation.check_postcode_column(df, 0)

    # Assert the correct rows were filtered out
    expected_data = {'col1': ['SE1 1AA', 'PR2 9HT']}
    expected_df = pd.DataFrame(expected_data)
    
    pd.testing.assert_frame_equal(valid_df, expected_df)