"""
Contains functions for cleaning, formatting, and processing raw data acquired from running `data_acquisition.py`.
This includes all NLP text processing steps.

Stephen Kaplan, 2020-08-10
"""
import pandas as pd


def format_metadata():
    """
    Formats all text data in raw standup comedy metadata dataframe.
    """
    df_metadata = pd.read_pickle('data/raw_standup_comedy_metadata.pkl')

    # convert all text data to have only first letter capitalized
    df_metadata['Comedian'] = df_metadata['Comedian'].str.title()
    df_metadata['Title'] = df_metadata['Title'].str.title()

    # persist formatted metadata dataframe
    df_metadata.to_pickle('data/standup_comedy_metadata.pkl')


if __name__ == "__main__":
    format_metadata()
