import os
import pandas as pd
import re
from data import DataRow, DataClass, ComparisonData, BaseDataClass, write_formatted
from notificationtype import NotificationType
from location import LocationPoint
from utils import file_utils
from sign_plan import SignPlan
from value_extractor import ValueExtractor
from comparison_result import ComparisonResult
from track_length_helper import TrackLengthHelper
from location_track_helper import LocationTrackHelper
from httpclient import HttpClient
from typing import Dict
from processing_error import ProcessingError


class ExcelTransformer:

    """
    Class for transforming signplan excels to csv files.
    """

    def __init__(self, new_columns):
        self.columns = new_columns
        self.sign_plan: SignPlan = None
        self.df: pd.DataFrame = None
        self.add_data: DataClass = None
        self.remove_data: DataClass = None
        self.change_data: DataClass = None
        self.manual_data: DataClass = None
        self.no_data: DataClass = None

        self.is_initialized = False

        self.remove_columns = [("asset_type","Omaisuuslaji (pakollinen)"),
                               ("existing_asset_id","Nykyisen kohteen id (pakollinen)"),
                               ("route_number","Ratanumero (pakollinen)"),
                               ("location_track","Sijaintiraide (pakollinen)"),
                               ("reason","Poiston syy (pakollinen)"),
                               ("action_additional_info","Lisätiedot")]

        self.httpclient = HttpClient()
        self.kilometer_info = TrackLengthHelper(self.httpclient)
        self.location_track_helper = LocationTrackHelper(self.httpclient)


        self.ratko_data: pd.DataFrame = None
        self.extractor = ValueExtractor(self.httpclient,self.location_track_helper)
        self.comparison_results: list[ComparisonResult] = []
        self.row_messages: Dict[int,str] = {}
        self.manual_rows = set()


    def clear_state(self, clear_ratko = False):
        """
        Clears all signplan specific data. By default does not clear ratko_data 
        as it is normally used for multiple sign plans. 
        """
        self.sign_plan = None
        self.df = None
        self.add_data.clear()
        self.remove_data.clear()
        self.change_data.clear()
        self.manual_data.clear()
        self.comparison_results.clear()
        self.row_messages.clear()
        self.manual_rows.clear()
        self.no_data.clear()


        if clear_ratko:
            self.ratko_data = None

        self.is_initialized = False

    def initialize(self,plan: SignPlan, ratko_data: pd.DataFrame):
        
        """
        Initializes transformer for the processing of new signplan. 
        """

        self.sign_plan = plan
        self.df = plan.get_df()
        self.ratko_data = ratko_data

        self.add_data = DataClass(self.sign_plan.get_filename(),NotificationType.ADD_NOTIFICATION, self.columns)
        self.remove_data = DataClass(self.sign_plan.get_filename(),NotificationType.REMOVE_NOTIFICATION, self.columns)
        self.change_data = DataClass(self.sign_plan.get_filename(),NotificationType.CHANGE_NOTIFICATION, self.columns)
        self.manual_data = DataClass(self.sign_plan.get_filename(),NotificationType.ADD_NOTIFICATION, self.columns) # manual data uses same columns as add-data
        self.no_data = DataClass(self.sign_plan.get_filename(),NotificationType.ADD_NOTIFICATION, self.columns)
        self.is_initialized = True

    def transform_excel(self):

        if not self.is_initialized:
            raise RuntimeError("Initialize should be called before transform_excel")

        print(f"Prosessoidaan: {self.sign_plan.get_filename()}")

        # Process each sign
        for idx, row in self.df.iterrows():

            # Compare to ratko-data to find possible match.
            try:
                # No need to do ratko comparison for manual processing signs as they will added to
                # manual lists regardless of the comparison result.
                if self._can_be_compared(idx,row) and not self._is_manual_processing_sign(idx,row):
                    comparison_result: ComparisonResult = self._compare_to_ratko(idx,row)
    
                    if comparison_result:
                        self.row_messages[idx] = comparison_result.get_note()
                        self.comparison_results.append(comparison_result)
                else:
                    comparison_result = None
                
                # Process all the columns
                columns = self._needed_columns(comparison_result)
                comparison_result, new_row = self._process_columns(idx,row,columns,comparison_result,True)

            except ProcessingError as err:
                self.row_messages[idx] = str(err)
                self.manual_rows.add(idx)
                continue

            self._insert_data(comparison_result, new_row,idx) # Add to corresponding container

        # If several rows are matched to a single ratko_sign, remove them from data containers
        # as they should be handled manually.
        reprocess_rows = self._remove_any_duplicate_matches()

        # Reprocess rows that were previously assigned to remove lists.
        for idx in reprocess_rows:
            row = self.df.iloc[idx]
            columns = self._needed_columns(None) # We use the most general set of columns
            _,datarow = self._process_columns(idx,row,columns,None,False)
            self._insert_data(None,datarow,idx)
        
        # Create all necessary files
        self._create_files()
        self.print_summary()

        self.clear_state() # Clear sinplan specific data after finished processing

    def _process_columns(self,
                        idx: int, 
                        row: pd.Series, 
                        columns: list[str], 
                        comparison_result: ComparisonResult, 
                        add_messages: bool = True):
        
        newrow = DataRow()
        
        for eng_col, fin_col in columns:
                    
            if eng_col == "existing_asset_id":
                # If there are more than one, the sign should have NO Notification type and this eng_col should not havebe encountered
                assert( comparison_result.has_unique_match()) 
                col_value = comparison_result.get_matches()[0]
            elif eng_col == "location_track":
                # If in function _compare_to_ratko the sign type was noticed to be
                # one of those not in ratko, this is the first time that location track is
                # searched for this sign so we might have and exception and the sign should be 
                # added to manually processed category.
                try:
                    obj = self.location_track_helper.location_track_info(row)
                    col_value = obj["name"]
                except ProcessingError as err:
                    col_value = ""
                    if add_messages:
                        comparison_result = None # Set to None to signal that this should be added to manual data.
                        self.row_messages[idx] = str(err)
                        self.manual_rows.add(idx)
            elif eng_col == "reason":
                if comparison_result is None: # Reason is not known for signs in manual list.
                    col_value = ""
                else:
                    notification_type = comparison_result.get_notification()
                    col_value = ("Kohteen poisto" 
                                if notification_type == NotificationType.REMOVE_NOTIFICATION
                                else "Puuttuva tieto" )
            else:
                # Handle rest of the columns with value extractor.
                col_value = self.extractor.get_value(row,(eng_col,fin_col))
            newrow.add_kv_pair(eng_col,col_value)

        # Add additional information after all the columns have been processed 
        if (comparison_result is None or 
            comparison_result.get_notification() == NotificationType.ADD_NOTIFICATION or
            comparison_result.get_notification() == NotificationType.NO_NOTIFICATION):
            newrow.add_kv_pair("additional_information", self.extractor.get_additional_information())

        return comparison_result,newrow # return the possibly modified comparison_result

    def print_summary(self):

        added_count = self.add_data.num_rows()
        remove_count = self.remove_data.num_rows()
        change_count = 0 # Not implemented currently
        manually_handled_count = len(self.manual_rows)
        no_notification_count = self.df.shape[0] - self.add_data.num_rows() - self.remove_data.num_rows() - manually_handled_count # Rest


        txt = """Manuaalisesti tarkastettavia: {}\nLisäysilmoituksia: {}\nPoistoilmoituksia: {}\nMuutosilmoituksia: {}\nEi ilmoituksia: {}
          """.format(manually_handled_count,added_count,remove_count,change_count,no_notification_count)
    
        print(txt)
        print("")

    def _needed_columns(self, comparison_result: ComparisonResult):
        """

        """
        if (comparison_result is None or comparison_result.get_notification() == NotificationType.ADD_NOTIFICATION or
            comparison_result.get_notification() == NotificationType.NO_NOTIFICATION):
            return [col for col in self.columns if col[0] != "existing_asset_id"] # add notifications dont have ids. 
        elif comparison_result.get_notification() == NotificationType.CHANGE_NOTIFICATION:
            return self.columns # Not used currently
        elif comparison_result.get_notification() == NotificationType.REMOVE_NOTIFICATION:
            return self.remove_columns

    def _is_manual_processing_sign(self,idx,row: pd.Series):

        """
        Certain signs are excluded from the processing.
        """

        prod_number = row["MERKIN VALMISTUSNUMERO"]
        sign_type = row["MERKKI/MERKINTÄ"]
        if (pd.notna(prod_number) and prod_number in ["T-259","T-261","T-261B", "T-262", "T-262A", "T-262B"] or
            pd.notna(sign_type) and sign_type in ["Junakulkutien päätekohta -merkki", "Junakulkutien päätekohta -tunnus"] ):

            self.row_messages[idx] = f"Valmistusnumero: {prod_number}" if pd.notna(prod_number) else f"Merkkityyppi: {sign_type}" 
            self.manual_rows.add(idx)
            
            return True

        return False

    def _can_be_compared(self,idx:int, row: pd.Series):

        """
        It is mandatory that certain columns have values so that the sign can be be compared to RATKO data.
        """

        for column in ["RATAKILOMETRI", "RATANUMERO", "TILIRATAOSA", "PUOLI", "LUKUSUUNTA", "TOIMENPIDE"]:
            if pd.isna(row[column]):
                self.row_messages[idx] = f"{column.capitalize()} tietoa ei saatavilla"
                self.manual_rows.add(idx)

                return False
        
        return True

    def _compare_to_ratko(self,idx: int, row: pd.Series):
        
        """
        Comparares row to ratko data.

        :return: Comparison result object.
        """
        not_in_ratko_indices = self._not_in_ratko()
        
        process_manually = False
        if not_in_ratko_indices[idx]:
            matches = []
        else:
            try:
                matches = self._try_find_OID_from_ratko_data(row) #  OID = 'Existing asset id'
            except ProcessingError as err:
                self.row_messages[idx] = str(err)
                self.manual_rows.add(idx)                
                process_manually = True

        comparison_result = ComparisonResult(matches,row, idx) if not process_manually else None

        return comparison_result

    def _try_find_OID_from_ratko_data(self,row: pd.Series) -> list[str]:

        """
        Tries to match merkkisuunnitelma row to ratko data to find an OID.

        :return: List of OIDs from matched rows. Can be empty.
        """

        matched_rows = self._find_matching_rows(row)

        if self.ratko_data[matched_rows].shape[0] == 0: # No matches
            matches = []
        elif self.ratko_data[matched_rows].shape[0] == 1: # Unique match
            match = self.ratko_data[matched_rows]["existing_asset_id"].squeeze()
            matches = [match]
        else: # Multiple matches
            matches = self._try_narrow_down_matches(row,matched_rows)

        return matches

    def _find_matching_rows(self,row: pd.Series) -> pd.Index:

        """
        Finds rows in ratko data that match to signplan row using a specific set of columns.
        :return: Matched rows as logical array. 
        """
        lower_boundary, upper_boundary = self._boundaries_for_range(row)
        
        accounting_route_number = row["TILIRATAOSA"]
        route_number = row["RATANUMERO"]
        side = row["PUOLI"].lower()
        facing_direction = row["LUKUSUUNTA"].lower()

        # Boolean array for row matching.
        bool_arr = (
            (self.ratko_data["accounting_route_number"].squeeze().str.contains(accounting_route_number)) &
            (self.ratko_data["route_number"].squeeze().str.contains(route_number)) &
            (self.ratko_data["side"].squeeze().str.lower().apply(lambda x: True if x == "ei tiedossa" else x == side) ) &
            (self.ratko_data["facing_direction"].squeeze().str.lower().apply(lambda x : True if x == "ei tiedossa" else x == facing_direction)) &
            (self.ratko_data["point"].squeeze().apply(LocationPoint.from_str) >= lower_boundary ) &
            (self.ratko_data["point"].squeeze().apply(LocationPoint.from_str) <= upper_boundary ) )
        
        # Some additional columns that are used conditionally.
        sign_type = row["MERKKI/MERKINTÄ"]
        if pd.notna(sign_type):
            # sign_type can be for example 'Vanha nopeusmerkki'.
            sign_type = sign_type.lower()
            sign_type = sign_type[len("vanha")+1:] if sign_type.startswith("vanha") else sign_type
            bool_arr = bool_arr & (self.ratko_data["track_sign_type"].squeeze().str.lower() == sign_type )  
        
        if row["TOIMENPIDE"].lower() != "merkki poistetaan": # 'Merkin valmistusnumero' is empty for 'Merkki poistetaan' rows 
            track_sign_production_number = row["MERKIN VALMISTUSNUMERO"]
            bool_arr = bool_arr & (self.ratko_data["track_sign_production_number"].squeeze() == track_sign_production_number )

        if pd.notna(row["MERKIN TEKSTI"]):
            numeric_part = re.findall("[0-9]+",row["MERKIN TEKSTI"])
            if len(numeric_part) == 1:
                bool_arr = bool_arr & ( self.ratko_data["track_sign_text"].squeeze().str.contains(numeric_part[0]) )

        return bool_arr
    
    def _boundaries_for_range(self,row: pd.Series, range_m: int = 30):

        """
        Determines lower and upper boundaries -/+ range_m meters from the current point. Takes into account that 
        not every track kilometer has length of 1000 m.
        """

        try:
            current_point = LocationPoint.from_str(row["RATAKILOMETRI"])
        except ValueError:
            raise ProcessingError("RATAKILOMETRIA ei voitu prosessoida")

        location_track_info = self.location_track_helper.location_track_info(row)
        location_track_OID = location_track_info["id"]
        
        try:
            # Length of the current track kilometer
            meters_in_track_kilometer = self.kilometer_info.meters_for_track_kilometer(location_track_OID,current_point.kilometers())
        except Exception:
            raise ProcessingError("Virhe: sijaintiraiteen pituutta ei voitu määrittää")

        if current_point.meters() + range_m > meters_in_track_kilometer:
            # If the next track kilometer is very short, in theory it would be possible that we would
            # go already to the track kilometer after the next. So here we would check the length of the next track kilometer. 
            # However, it is very unlikely as it would require that we are right at the end of current track kilometer
            # and the next one had length less than range_m meters.
            meters = current_point.meters() + range_m - meters_in_track_kilometer
            upper_boundary = LocationPoint(current_point.kilometers()+1,meters)
        else:
            upper_boundary = current_point.create_from_this(add_meters=range_m)

        if current_point.meters() < range_m:
            try:
                # Find the length of the previous track kilometer in order to calculate the lower boundary.
                meters_in_prev_track_km = self.kilometer_info.meters_for_track_kilometer(location_track_OID,current_point.kilometers()-1)
                meters_left = range_m - current_point.meters() # The part we still need to subtract from full track kilometer.
                lower_boundary = LocationPoint(current_point.kilometers()-1,meters_in_prev_track_km-meters_left)
            except KeyError:
                # We have gone ouside of the location track. Use start of current track kilometer as lower boundary.
                lower_boundary = LocationPoint(current_point.kilometers(),0)

        else:
            lower_boundary = current_point.create_from_this(add_meters=-range_m)

        return lower_boundary, upper_boundary
    
    def _try_narrow_down_matches(self,row: pd.Series, bool_arr: pd.Index) -> list[str]:

        """
        In case of multiple matches, tries to narrow the number of matches using rail number.
        :return: Matched OIDs as list
        """

        matches = self.ratko_data[bool_arr]["existing_asset_id"].squeeze()

        rail_number = row["RAIDE"]
        if pd.notna(rail_number):
            num_match = re.findall("[0-9]+",rail_number)
            if len(num_match) == 1:
                bool_arr = bool_arr & (self.ratko_data["location_track"].squeeze().str.contains(num_match[0]))
                matches = self.ratko_data[bool_arr]["existing_asset_id"].squeeze()

        # Matches reduces to string if there is only one row.
        return [matches] if isinstance(matches, str) else matches.tolist()

    def _create_files(self):
        
        path = file_utils.create_dir_if_not_exist("prosessoidut")
        
        # Create dataframe of matches.
        comparison_data = ComparisonData(self.sign_plan.get_filename(),self.ratko_data,self.comparison_results)

        data: list[BaseDataClass] = [self.add_data,self.remove_data,self.change_data,comparison_data]
        for d in data:
            d.write_to_file(path)

        # Create dataframe of manually processed signs.
        if self.manual_data.num_rows() > 0:
            cols = [] 
            cols.append([col[0] for col in self.columns if col[0] != "existing_asset_id"])
            cols.append([col[1] for col in self.columns if col[1]  != "Nykyisen kohteen id (pakollinen)"])
            dfm = pd.DataFrame(data=self.manual_data.get_data(), columns=cols)
            dfm.insert(0,"OPERAATIO",[self.row_messages[idx] for idx in sorted(list(self.manual_rows))])
            filepath = os.path.join(path,self.sign_plan.get_filename()+"_manuaalitarkastus.csv")
            dfm.to_csv(path_or_buf=filepath, sep=";", index=False,mode="wb",encoding="utf-8-sig")

        # Create summary file of the processed dataframe.
        self.df.insert(1,"OPERAATIO",list(self.row_messages.values()))
        
        # Add coloring to all rows manual rows.
        styling = file_utils.add_coloring(self.df,self.manual_rows) # 
        write_formatted(self.df,self.sign_plan.get_filename()+"_yhteenveto.xlsx", "MERKKISUUNNITELMA", styling)

        
    def _insert_data(self,comparison_result: ComparisonResult, row: DataRow, row_idx: int):

        """
        If the result of comparison requires, adds data to corresponding container.
        """

        if comparison_result is None:
            self.manual_data.add_row(row_idx,row)
            return

        match comparison_result.get_notification():
            case NotificationType.ADD_NOTIFICATION:
                self.add_data.add_row(row_idx,row)
            case  NotificationType.REMOVE_NOTIFICATION:
                self.remove_data.add_row(row_idx,row)
            case NotificationType.CHANGE_NOTIFICATION:
                self.change_data.add_row(row_idx,row)
            case NotificationType.NO_NOTIFICATION:
                self.no_data.add_row(row_idx,row)
            case default:
                pass

    def _not_in_ratko(self) -> pd.DataFrame:

        """
        Compares signplan dataframe to ratko-data. If there are track sign production numbers 
        (example: 'T-138A') in the signplan file which are not found in ratko_data,
        there will be no matches for those rows and self._try_find_OID_from_ratko_data
        does not need to be called.
        """

        excel_track_signs = self.df["MERKIN VALMISTUSNUMERO"]
        # RATKO data is multi-index. Use squeeze to get a series object.
        ratko_signs = self.ratko_data["track_sign_production_number"].squeeze() 
        
        # Get boolean array indicating rows that have track sign production numbers which are 
        # not found in ratko data and are not NaN. 
        bool_arr = ~excel_track_signs.isin(ratko_signs) & excel_track_signs.notna()

        return bool_arr

    def _remove_any_duplicate_matches(self):

        """
        Goes through matches and finds if there are ratko signs that have more than
        one signplan sign assigned to them. If any are found, removes
        any corresponding rows added to add or remove-data as these cases
        should be handled manually.  
        """

        from collections import Counter

        # Find all OIDs which have multiple signs assigned to them.
        counts = Counter(oid for results in self.comparison_results for oid in results.get_matches())
        duplicates = [oid for oid,count in counts.items() if count > 1]

        reprocess_indices = []
        for oid in duplicates:
            for comp_result in self.comparison_results:
                if oid in comp_result.get_matches():
                    idx = comp_result.get_idx()
                    self.manual_rows.add(idx)
                    msg = "Useampi merkkisuunnitelma rivi kohdistui samalle ratko-merkille."
                    self.row_messages[idx] = msg
                    comp_result.set_note(msg)

                    # The columns of the signs that were initially assigned to remove lists need to be
                    # reprocessd as remove list signs dont have same information as add_data. 
                    if comp_result.get_notification() == NotificationType.REMOVE_NOTIFICATION:
                        reprocess_indices.append(comp_result.get_idx())
                    else:
                        row = None
                        # Removed row should added to manual list instead.
                        for data in [self.add_data,self.no_data]:
                            row = data.get_by_idx(idx)
                            if row:
                                break
                        if row is not None:
                            self.manual_data.add_row(idx,row)

                        self.remove_data.remove_by_row_idx(idx)
                        self.add_data.remove_by_row_idx(idx)

        
        return reprocess_indices