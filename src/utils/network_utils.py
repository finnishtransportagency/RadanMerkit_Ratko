import os
from datetime import datetime, timezone


raide_base_url = os.getenv("RAIDE_URL")
if raide_base_url is None:
    raide_base_url = "https://api.vayla.fi/raide/ratarekisteri/rest/api"

def query_infra_api_with_OID(OID: str, endpoint: str, property_names: list = []) -> dict:

    """
    Proxy function for queries with OID.

    :return: Result as json-dict.

    """

    endpoint = endpoint + "/" + OID
    return infra_url(endpoint,property_names)


def infra_url(endpoint: str,
              property_names: list = []) -> dict:

    """
    Creates url to make queries to given infra-api endpoint.

    :endpoint: Infra APi endpoint to be queried.
    :property_names: Fields to be included in returned json. Empty ist to get all the fields.
    """

    property_param = "propertyName=" + ",".join(property_names) + "&" if len(property_names) > 0 else ""
    base_url = "https://rata.digitraffic.fi/infra-api/0.7/"
    datenow = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = base_url + endpoint +  f".json?{property_param}time={datenow}/{datenow}"

    return url

def route_OID_endpoint(route_number: str) -> str:

    return f"/converter/v1.0/routenumberOID/routenumberName/{route_number}"

def location_tracks_endpoint(route_OID: str) -> str:

    return f"/locations/v1.1/locationtracks/routenumber/{route_OID}?state=IN%20USE"

def zerometer_points_endpoint(location_track_OID: str):
    return f"/locations/v1.1/zeroMeterPoints/{location_track_OID}"

def track_sign_properties_endpoint():

    return f"/metadata/v1.0/assetTypes/track_sign/properties"

def raide_url(endpoint: str):

    if raide_base_url is None:
        raise ValueError("Raide-API urlia ei löytynyt.")

    return raide_base_url + endpoint
