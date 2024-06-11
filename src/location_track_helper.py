import re
import pandas as pd
import math
from location import LocationPoint
from httpclient import HttpClient
from processing_error import ProcessingError

class LocationTrackHelper:
    """
    Handles location track specific information
    """

    def __init__(self, httpClient):
        self.tracks = {"location_tracks": {}}
        self.routenum_to_location_tracks = {} # Maps routenumber to its location tracks.
        self.httpclient: HttpClient = httpClient


    def location_track_info(self, row: pd.Series):
        """
        gets location track for a given route number. If not present in the dictionary,
        fetches the information from Raide-API.
        """

        route_number = row.get("RATANUMERO")
        if pd.isna(route_number):
            raise ProcessingError("Ratanumero tietoa ei saatavilla")

        if not route_number in self.routenum_to_location_tracks:
            self._fetch_location_tracks(route_number)

        return self._find_location_track(row)

    def _fetch_location_tracks(self,route_number):

        """
        Fetches all location tracks for given route number and stores them into member variable.
        """

        from utils.network_utils import route_OID_endpoint, location_tracks_endpoint, raide_url
        from auth import raide_api_headers

        # Headers required for Raide API.
        raide_headers = raide_api_headers()

        # Fetch OID information for the route
        oid_endpoint = route_OID_endpoint(route_number)
        result = self.httpclient.get(raide_url(oid_endpoint),raide_headers)
        route_OID = result["value"]
        
        # Get all locations tracks of the route using route OID.
        locationtracks_endpoint = location_tracks_endpoint(route_OID)
        tracks = self.httpclient.get(raide_url(locationtracks_endpoint),raide_headers)

        self.routenum_to_location_tracks[route_number] = tracks

    def _find_location_track(self,row: pd.Series):

        try:
            point = LocationPoint.from_str(row["RATAKILOMETRI"])
        except Exception:
            raise ProcessingError("RATAKILOMETRIA ei voitu prosessoida")
        
        if pd.isna(row["RAIDE"]):
            raise ProcessingError("Sijaintiraidetta ei voitu määrittää. Raidetieto puuttuu.")

        
        # Get all location tracks of the route filtered by the accounting route number.
        all_matches = []
        tracks = self.routenum_to_location_tracks[row["RATANUMERO"]]
        tracks = [t for t in tracks if "areas" in t] # In some rare cases there is no area information.
        tracks = [t for t in tracks 
                            for a in t["areas"] 
                            if a["areaType"] == "accounting_route_number" and
                            a["name"] == row["TILIRATAOSA"]
                            ]

        if len(tracks) == 0:
            raise ProcessingError(f"Ratanumerolle {row['RATANUMERO']} ei löytynyt sijaintiraiteita.")

        if row["RAIDE"] == "Linjaraide":
            
            # Linjaraide contains route number
            result = [t for t in tracks if row["RATANUMERO"] in t["name"]] 

            # Should be unique and the sign should be located on track:
            if len(result) == 1 and self._location_is_on_track(point,result[0]):
                return result[0]
            else:
                raise ProcessingError("Linjaraidetta ei voitu määrittää.")
            
        track_candidates = [t for t in tracks if self._location_is_on_track(point,t)]



        if len(track_candidates) > 0:
            
            filtered_tracks = [t for t in track_candidates if row["RAIDE"] in t["name"] ]

            if len(filtered_tracks) > 1:
                raise ProcessingError("Sijaintiraidetta ei voitu määrittää yksiselitteisesti")
            elif len(filtered_tracks) == 1:
                return filtered_tracks[0]
            
            if len(track_candidates) > 1:
                raise ProcessingError("Sijaintiraidetta ei voitu määrittää yksiselitteisesti")
            return track_candidates[0]
        else:
            raise ProcessingError("Sijaintiraidetta ei voitu määrittää")

    
    def _location_is_on_track(self,point: LocationPoint, track_info: dict):
        """
        Checks whether the given point is inside the start and end points of a given track.
        :return: True if is inside, false otherwise.
        """

        start_node = next(n["point"] for n in track_info["nodecollection"]["nodes"] if n["nodeType"] == "start_point")
        end_node = next(n["point"] for n in track_info["nodecollection"]["nodes"] if n["nodeType"] == "end_point")


        start_m = math.floor(float(start_node["m"]))
        end_m = math.ceil(float(end_node["m"]))

        track_start_point = LocationPoint(int(start_node["km"]),start_m)
        track_end_point = LocationPoint(int(end_node["km"]),end_m)


        return point >= track_start_point and point <= track_end_point



