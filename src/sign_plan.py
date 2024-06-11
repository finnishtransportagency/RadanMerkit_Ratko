import pandas as pd

class SignPlan:

    def __init__(self,df: pd.DataFrame, filename: str):
        self.df = df
        self.filename = filename
        self.row_to_matches = {}

    def get_df(self):
        return self.df
    def get_filename(self):
        return self.filename
    
    def add_matches(self,row: int, matches: list[str]):
        self.row_to_matches[row] = matches

    def nrows(self):
        return self.df.shape[0]