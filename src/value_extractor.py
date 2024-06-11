
import pandas as pd
from datetime import datetime, timezone
from typing import Tuple, Union
from utils.network_utils import infra_url
from allowed_values import AllowedValues

class ValueExtractor:

    """
    Class containing logic for extracting the values from sign plan rows.
    """

    def __init__(self, httpclient, location_track_helper):
        self.httpclient = httpclient
        self.location_track_helper = location_track_helper
        self.allowed_values = AllowedValues(self.httpclient)
        self.data = {
            "location_tracks": {}
        }

        # Columns that are always set to same value.
        self.constants =  {
            "asset_state": "IN USE",
            "coordinateEast3067": "",
            "coordinateNorth3067": "",
            "coordinateEast4326": "",
            "coordinateNorth4326": "",
            "cacheCoordinateEast3067": "",
            "cacheCoordinateNorth3067": "",
            "cacheCoordinateEast4326": "",
            "cacheCoordinateNorth4326": "",
            "additional_information": "",
            "estimated_commissioning_date": "",
            "estimated_decommissioning_date": "",
            "geomAccuracyType": "",
            "maintenance_district_override": "",
            "owner": "Väylävirasto",
            "asset_type": "track_sign",
            "production_year": "1900",
            "swiveling" : "Ei tiedossa",
            "track_sign_id": "",
            "notes" : "Ei tiedossa",
            "vr_track_id": "",
            "warranty_end_date": "",
            "action_additional_info": ""
        }

        # Maps RATKO column to corresponding finnish signplan column.
        self.RATKO_to_signplan= {
            "installation_structure": "KIINNITYS",
            "installation_direction": "ASENNUSSUUNTA",
            "facing_direction": "LUKUSUUNTA",
            "foundation": "PERUSTUS",
            "track_sign_production_number": "MERKIN VALMISTUSNUMERO",
            "track_sign_type": "MERKKI/MERKINTÄ",
            "point": "RATAKILOMETRI", 
            "exactPoint": "RATAKILOMETRI",
            "track_sign_fastening": "KIINNITYSTARVIKKEET",
            "side": "PUOLI"
        }

        # Columns that cant be empty and needs to be marked "Ei tiedossa" in case of no value.
        self.not_known_if_nan = [
            "facing_direction",
            "installation_structure",
            "installation_direction",
            "side",
            "track_sign_production_number",
            "track_sign_type",
            "foundation"
            ]
        
        # Columns for which RATKO accepts only for instance integer values but whose corresponding
        # signplan columns often have more complex content and thus cannot be used as is.
        # Contents of these columns will be added to additional_information column.
        self.additional_information_columns = ["installation_height", "installation_distance"]
        
        # Additional information will contain values from several excel columns.
        self.additional_information = dict()

        # Columns that need some specific logic, api-calls etc. to get the value.
        self.special_cases = {
            "maintenance_oversight_district": self._handle_maintenance_oversight_district,
            "track_sign_fastening": self._handle_track_sign_fastening,
            "track_sign_fastening_if_other": self._handle_track_sign_fastening_if_other,
            "name": self._handle_name,
            "track_sign_text": self._handle_track_sign_text,
            "editedDate": self._handle_date,
            "effective_date": self._handle_date,
            "installation_structure": self._handle_installation_structure,
            "plug": self._handle_plug,
            "foundation": self._handle_foundation,
            "track_sign_production_number": self._handle_track_sign_production_number,
            "track_sign_type": self._handle_track_sign_type,
            "side": self._handle_side,            }

    def get_value(self,row:pd.Series,column: Tuple[str, str]):
        """
        Extracts the value corresponding the column name.
        """
        eng_col, fin_col = column

        if eng_col in self.constants:
            col_value = self.constants[eng_col] # return
        elif eng_col in self.additional_information_columns:
            self._add_to_additional_information(row,fin_col)
            col_value = ""
        elif self.allowed_values.is_enum_property(eng_col):
            signplan_col = self.RATKO_to_signplan[eng_col]
            result = self.allowed_values.is_allowed_enum_value(eng_col,row.get(signplan_col))
            if result:
                return result
            # Check if its one of the special cases.
            if eng_col in self.special_cases:
                col_value = self._handle_special_case(eng_col,row)
            else:
                col_value = None
        elif eng_col in self.special_cases: # There are also some non-enum special cases.
            col_value= self._handle_special_case(eng_col,row)
        else:
            # Some columns have different names in new format.
            if eng_col in self.RATKO_to_signplan:
                actual_name = self.RATKO_to_signplan[eng_col]
            else:
                # Finnish column name can be used as is
                actual_name = self._clean_finnish_column(fin_col) 
            
            col_value = row.get(actual_name.upper(),None) # Signplans use uppercase names

        # Take care of the missing values.
        if pd.isna(col_value):
            col_value = "Ei tiedossa" if eng_col in self.not_known_if_nan else ""

        return col_value

    def get_additional_information(self):
        """
        Creates string of column name: column value pairs containing information
        from several columns to be put into RATKO's additional_information column 
        """

        pairs = [f"{col.capitalize()}: {value}" for col, value in self.additional_information.items()]

        # Clear the contents for the next row
        self.additional_information.clear()

        # Add information that the sign is added by script processing
        note = "Tämän merkit tiedot on tuotettu skriptillä. "

        return note + ", ".join(pairs)
    
    def _handle_special_case(self,column: str, row: pd.Series) -> any:
        
        return self.special_cases[column](row)
    
    def _add_to_additional_information(self, row:pd.Series, column: str):
        """
        Stores value of column in additional information dict if its not empty.
        Uses finnish name for columns.
        """
        
        fin_col = self._clean_finnish_column(column)
        value = row[fin_col.upper()]

        if pd.isna(value):
            return
        
        self._add_to_additional_information_kv(fin_col,value)

    def _add_to_additional_information_kv(self, key, value):
        """
        Stores value with key in additional information dict.
        """
        
        self.additional_information[key] = value

    def _clean_finnish_column(self,column: str) -> str:
        """
        Cleans the finnish column.
        :return: Cleaned column name.
        """
        
        idx = column.find("(pakollinen)") # Remove (pakollinen) from the end if exists

        if idx != -1:
            column = column[:idx]
        column = column.strip().lower()

        return column

    # Function to form value for maintenance_oversight district (isannointialue)
    def _handle_maintenance_oversight_district(self,row: pd.Series) -> Union[str, None]:

        """
        Gets value for column  maintenance_oversight_district.

        First queries infra-api to get maintenance districts (kunnossapitoalueet).
        Gets only fields name and maintenance_oversight_district.
        Name is for instance "Alue 1: Uusimaa". Using the correct maintenance district, OID for 
        maintenance oversight district is used to get name for maintenance oversight district.
        The results are stored in self.data.

        :param: row
        :return: Name of the maintenance oversight district or None if not found.

        """

        if "maintenance_districts" in self.data:
            maintenance_districts = self.data["maintenance_districts"]
        else:
            maintenance_districts = self.httpclient.get(
                infra_url(endpoint="kunnossapitoalueet", property_names=["nimi","isannointialue"])) 
            self.data["maintenance_districts"] = maintenance_districts

        
        m_district = row.get("KUNNOSSAPITOALUE")
        if pd.isna(m_district):
            return None
        # Go through maintenance districts to find OID for corresponding maintenance oversight district.
        maintenance_o_district_OID = next((v[0]["isannointialue"] for v in maintenance_districts.values() if m_district in v[0]["nimi"]),None)

        if maintenance_o_district_OID is None: # Not likely
            return None
        
        if "maintenance_o_districts" in self.data:
            maintenance_o_districts = self.data["maintenance_o_districts"]
        else:
            maintenance_o_districts = self.httpclient.get(
                infra_url(endpoint="isannointialueet", property_names=["nimi","tunniste"]))
            self.data["maintenance_o_districts"] = maintenance_o_districts

        # Find name of the maintenance_oversight_district corresponding to the OID.
        obj = maintenance_o_districts.get(maintenance_o_district_OID,None)

        return obj[0]["nimi"] if obj is not None else obj
    
    def _handle_track_sign_fastening(self,row: pd.Series) -> Union[str, None]:

        value = row.get("KIINNITYSTARVIKKEET")

        if pd.isna(value):
            return None
        
        return "Muu"
    
    def _handle_track_sign_fastening_if_other(self,row: pd.Series) -> str:

        if self._handle_track_sign_fastening(row) == "Muu":
            return row["KIINNITYSTARVIKKEET"]
        
        return ""

    def _handle_name(self,row: pd.Series) -> Union[str, None]:

        """
        Name is a combination of location_track, exact point and track sign type columns.
        """

        if pd.isna(row["MERKKI/MERKINTÄ"]):
            return None
        try:
            track_info = self.location_track_helper.location_track_info(row)
        except Exception:
            track_info = None
        point = row.get("RATAKILOMETRI",None)
        track_sign_type = row.get("MERKKI/MERKINTÄ",None)

        if track_info is None or point is None or track_sign_type is None:
            return None

        return track_info["name"] + " " + point + " " + track_sign_type

    def _handle_track_sign_text(self,row: pd.Series):

        text = row.get("MERKIN TEKSTI")
        sign_type = row.get("MERKIN VALMISTUSNUMERO")

        if sign_type == "T-115" or sign_type == "T-115A":
            return "JKV"
        elif pd.isna(text) or len(text) == 0 or text.isspace():
            return "Ei ole"
        elif text == "Erillisen liitteen mukaan":
            return "Ei tiedossa"

        return text

    def _handle_date(self,row: pd.Series) -> str:

        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
    
    def _handle_installation_structure(self,row:pd.Series):
        
        value = row.get("KIINNITYS")

        if pd.isna(value):
            return None
        
        # Place often contains useful information for the installation structure field.
        place = row.get("PAIKKA")

        if value in ["Nykyinen D60 pylväs","Nykyinen d60 pylväs", "Uusi D60 pylväs","Uusi d60 pylväs", "Nykyinen merkkipylväs, d60"]:
            return "Merkkipylväs, 60mm halk."
        elif value == "Nykyinen D110 pylväs" or (pd.notna(place) and "oma pylväs" in place.lower()):
            return "Merkkipylväs"
        elif pd.notna(place) and "Ratajohtopylväs" in place:
            return "P-pylväs"
        elif value in ["Opastinpylväs, pyöreän muotoinen"]:
            return "Opastinpylväs, pyöreä"
        elif value in ["Opastinpylväs, suorakulmainen","Opastinpylväs, suorakaiteen muotoinen"]:
            return "Opastinpylväs, kantikas"
        elif ( value in ["Pääopastimen päälle"] or
            (pd.notna(place) and (place.startswith("Pääopastin") or place.startswith("Esiopastin")))):
            return "Opastinpylväs"
        elif value in ["Valaisinpylväs, pyöreä"]:
            return "Valaisinpylväs"
        elif value in ["Kiskon varsi tai jalka'"]:
            return "Kiskovarsi"
        else:
            return "Muu"

    def _handle_plug(self,row: pd.Series):

        value = row.get("TULPPA")

        if pd.isna(value):
            return None
        
        if value.isnumeric():
            return value
        elif value.lower() == "kyllä":
             return "1"
        else:
            return ""

    def _handle_foundation(self,row: pd.Series):
        
        value = row.get("PERUSTUS")

        if pd.isna(value):
            return None
        
        if "betoni" in value:
            return "Betoniperustus"
        
        return None
    
    def _handle_track_sign_production_number(self, row: pd.Series):

        value = row.get("MERKIN VALMISTUSNUMERO")

        if pd.isna(value):
            return None
        
        # Some problematic cases
        if "A29.1" in value or "A29" in value:
            return "176"
        elif "A29.2" in value:
            return "177"
        elif value == "T-130":
            return "T-130A"
        elif value == "T-197":
            return "T-197A"
        elif value == "T-252A":
            return "T-252"
        elif value in ["H8", "H8 (823)"]:
            self._add_to_additional_information_kv("Valmistusnumero","Tieliikenteen merkki, sähköjohdon korkeus")
            return "H24 Normaali koko"
        elif value in ["B6 (232)"]:
            self._add_to_additional_information_kv("Valmistusnumero","Tieliikenteen merkki, pakollinen pysähtyminen")            
            return "H24 Normaali koko"
        elif value == "-":
            return None

        return value
    
    def _handle_track_sign_type(self, row:pd.Series):

        value = row.get("MERKKI/MERKINTÄ")

        if pd.isna(value):
            return None
        
        value = value.lower()
        if value == "ensimmäisen luokan liikenteenohjaus":
            return "1. luokan liikenteenohjaus"
        elif value == "toisen luokan liikenteenohjaus":
            return "2. luokan liikenteenohjaus"
        elif value == "valtion rataverkon raja":
            return "Valtion rataverkon rajamerkki"
        elif value == "vaunujen kohdistuspaikkamerkintä":
            return "Vaunujen kohdistuspaikkamerkki"
        
        return None
    
    def _handle_side(self, row: pd.Series):

        value = row.get("PUOLI")

        if pd.isna(value):
            return  None

        sign_type = row["MERKKI/MERKINTÄ"]
        if sign_type == "Ryhmityseristinmerkki":
            return "Yläpuolella"
        
        return None

