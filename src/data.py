import os
import numpy as np
from abc import ABC, abstractmethod
import pandas as pd
from notificationtype import NotificationType
from comparison_result import ComparisonResult
from typing import Dict

def write_formatted(df: pd.DataFrame,filename: str, sheetname: str, styling = None):

    """
    Writes dataframe to excel file using formatting for better readability.
    """
    path = os.path.join(os.getcwd(),"prosessoidut")
    if not os.path.exists(path):
        os.makedirs(path)

    writer = pd.ExcelWriter(os.path.join(path,filename), engine="xlsxwriter")

    # Apply styles to excel if provided.
    if styling:
        styling.to_excel(writer, sheet_name=sheetname, index=False)
    else:
        df.to_excel(writer, sheet_name=sheetname, index=False)

    worksheet = writer.sheets[sheetname]  # pull worksheet object
    workbook = writer.book
    cell_format = workbook.add_format() # 
    cell_format.set_align("center")

    for idx, col in enumerate(df):  # loop through all columns
        series = df[col]
        max_len = max((
            series.astype(str).map(len).max(),  # len of largest item
            len(str(series.name))  # len of column name/header
            )) + 3  # adding a little extra space
        worksheet.set_column(idx, idx, max_len,cell_format)  # set column width
    writer.close()



class DataRow:

    """
    Class representing any generic row in excel or csv-file.
    """

    def __init__(self):
        """
        idx refers to signplan row_idx in case this row is built based on actual sign in signplan.
        """
        self.data = {}

    def add_kv_pair(self,key,value):
        self.data[key] = value

    def get_data(self):
        return self.data

    def values(self):
        return list(self.data.values())
    
    def __len__(self):
        return len(self.data)


class BaseDataClass(ABC):

    """
    Base class for all data containers.
    """

    def __init__(self):
        self.data: dict[int,DataRow] = {}
        self.columns = []
        self.filename = ""

    @abstractmethod
    def num_rows(self):
        pass
    
    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def write_to_file(self):
        pass
    
    def get_columns(self):
        return self.columns
    
    def clear(self):
        self.data.clear()
    
    def get_filename(self):
        return self.filename


class DataClass(BaseDataClass):

    def __init__(self,name:str, notification_type: NotificationType, column_names: list):

        self.data: Dict[int, DataRow] = {}
        self.notification_type = notification_type
        self.filename = name
        self.columns = []
        if self.notification_type == NotificationType.ADD_NOTIFICATION:
            # No id field for new signs
            self.columns.append([c[0] for c in column_names if c[0]!= "existing_asset_id" ])
            self.columns.append([c[1] for c in column_names if c[1]!= "Nykyisen kohteen id (pakollinen)" ])
    
        elif self.notification_type == NotificationType.CHANGE_NOTIFICATION:
            self.columns.append([c[0] for c in column_names ])
            self.columns.append([c[1] for c in column_names])
        elif self.notification_type == NotificationType.REMOVE_NOTIFICATION:
            cols =  {
                "asset_type":"Omaisuuslaji (pakollinen)",
                "existing_asset_id":"Nykyisen kohteen id (pakollinen)",
                "route_number":"Ratanumero (pakollinen)",
                "location_track":"Sijaintiraide (pakollinen)",
                "reason":"Poiston syy (pakollinen)",
                "action_additional_info":"Lisätiedot"
                }
            self.columns.append(list(cols.keys()))
            self.columns.append(list(cols.values()))
    
    def num_rows(self):
        return len(self.data)
    
    def get_data(self):
        """ Returns data rows sorted by idx. """
        d = {key:self.data[key] for key in sorted(self.data)}
        return [row.values() for _,row in d.items()]
    
    def get_by_idx(self,idx):
        """ Returns datarow by row idx. """

        return self.data.get(idx)
    
    def add_row(self, row_idx: int, row: DataRow):
        """
        Adds row with row_idx to container.
        """
        self.data[row_idx] = row

    def remove_by_row_idx(self,row_idx: int):
        """ If datarow exists for row_idx, removes it. If not found, does nothing."""

        if row_idx in self.data:
            del self.data[row_idx]
    
    def write_to_file(self,path):
        if len(self.data) == 0:
            return
        df = pd.DataFrame(data=self.get_data(), columns=self.columns)
        # Append ' to force excel to show numeric values as strings.
        df = df.map(lambda x: "'" + x if x != "" else x,na_action="ignore") 

        filepath = os.path.join(path,self.filename + "_" + NotificationType.to_filename(self.notification_type))
        df.to_csv(path_or_buf=filepath, sep=";", index=False,mode="wb",encoding="utf-8-sig")

class ComparisonData(BaseDataClass):

    def __init__(self, 
                 name: str, 
                 ratko_data: pd.DataFrame, 
                 results: list[ComparisonResult]):
        self.filename =  name + "_vertailu.xlsx"
        self.data = []

        # Columns for creating comparison dataframe.
        self.mappings = {
            "existing_asset_id": "MERKKISUUNNITELMAMERKKI", # No existing asset id in merkkisuunnitelma-data.
            "accounting_route_number": "TILIRATAOSA",
            "route_number": "RATANUMERO",
            "location_track": "RAIDE",
            "point": "RATAKILOMETRI",
            "side": "PUOLI",
            "facing_direction": "LUKUSUUNTA",
            "track_sign_production_number": "MERKIN VALMISTUSNUMERO",
            "track_sign_type": "MERKKI/MERKINTÄ",
            "track_sign_text": "MERKIN TEKSTI",
        }

        self.columns = list(self.mappings.keys())

        for comp_result in results:

            if not comp_result.has_matches():
                continue

            excel_row = comp_result.get_row()
            new_row = DataRow()

            new_row.add_kv_pair("RIVINUMERO", excel_row["RIVI"])
            for ratko_column, excel_column in self.mappings.items():

                cell = excel_row[excel_column] if excel_column != "MERKKISUUNNITELMAMERKKI" else excel_column
                new_row.add_kv_pair(ratko_column,cell)
            new_row.add_kv_pair("OPERAATIO",comp_result.get_note())
            self.data.append(new_row)

            # Add all matching ratko rows after the current sign row.
            match_list = comp_result.get_matches()    
            for oid in match_list:
                ratko_row = ratko_data[ratko_data["existing_asset_id"].squeeze() == oid]
                ratko_values = [ratko_row.squeeze()[c].values[0] for c in self.columns]
                
                # Construct a new row.
                new_row = DataRow()

                # Put the row num column at the beginnning of the row.
                row_num = ratko_row.index[0] +3 # +2 for header rows and +1 as excel row numbering starts from 1.
                new_row.add_kv_pair("RIVINUMERO",row_num)
                for k,v in zip(self.columns,ratko_values):
                    new_row.add_kv_pair(k,v)
                new_row.add_kv_pair("OPERAATIO", " ")
                self.data.append(new_row)
                    
            self.data.append(DataRow()) # Add an empty row between match groups.

        # Add additional non-data columns
        self.columns.insert(0,"RIVINUMERO")
        self.columns.append("OPERAATIO")
    
    def num_rows(self):
        return sum(1 for row in self.data if len(row) > 0)
    
    def get_data(self):

        return [row.values() for row in self.data]
    
    def write_to_file(self,filename):
        if len(self.data) == 0:
            return

        df = pd.DataFrame(data=self.get_data(),columns=self.get_columns())

        idx = pd.IndexSlice
        slice_ = idx[idx[df["OPERAATIO"].str.contains("MANUAALITARKASTUS",na=False) ], ['OPERAATIO']]
        style = df.style.apply(lambda s : np.tile("background-color: yellow",s.size),axis=1, subset=slice_)

        write_formatted(df,self.filename,"Vastaavuudet",style)