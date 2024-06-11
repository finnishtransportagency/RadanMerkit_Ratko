from httpclient import HttpClient

class TrackLengthHelper:

    def __init__(self, httpClient):
        self.location_track_to_kilometers = {}
        self.httpclient: HttpClient = httpClient

    def meters_for_track_kilometer(self, location_track_OID: str, track_kilometer: int) -> int:

        """
        Returns the length of given track kilomete in meters floored to nearest integer.
        """

        if not location_track_OID in self.location_track_to_kilometers:
            self._fetch_track_kilometers(location_track_OID)

        kilometer_info = self.location_track_to_kilometers[location_track_OID][track_kilometer]

        return int(kilometer_info["kmLength"])
    
    def _fetch_track_kilometers(self,location_track_OID: str):
        """
        Fetches kilometer info from raide api.
        """
        
        from utils.network_utils import zerometer_points_endpoint, raide_url
        from auth import raide_api_headers

        raide_headers = raide_api_headers()

        zerometerpoints = zerometer_points_endpoint(location_track_OID)
        result = self.httpclient.get(raide_url(zerometerpoints),raide_headers)

        # Map each track kilometer to its info object
        self.location_track_to_kilometers[location_track_OID] = {int(obj["point"]["km"]): obj for obj in result}