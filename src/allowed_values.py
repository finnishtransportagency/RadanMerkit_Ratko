import pandas as pd

class AllowedValues:

    """
    Fetches and holds information regarding to the properties of track signs.
    """

    def __init__(self, httpclient):
        self.httpclient = httpclient
        self.track_sign_properties = self._fetch_properties()

    def is_enum_property(self, property: str):
        """
        True if the given property is of enum type.
        """

        return property in self.track_sign_properties and self.track_sign_properties[property]["dataType"] == "ENUM"

    def is_allowed_enum_value(self,property: str, value: str):
        
        """
        Checks if the value is one of the allowed values of the given enum property.
        If it is, returns it in the correct textual form RATKO expects.

        If not, returns None.
        """

        if pd.isna(value):
            return None
        if property not in self.track_sign_properties:
            raise ValueError("Unknown track sign property: {property}")

        prop_obj = self.track_sign_properties[property]

        lowercase_values = prop_obj["enumList"]["lowerCaseValues"]
        if value.lower() in lowercase_values:
            return prop_obj["enumList"]["enumValues"][lowercase_values.index(value.lower())]["value"]
        
        return None


    def _fetch_properties(self):

        """
        Fetches all properties using RAIDE-API.
        """
        from utils.network_utils import raide_url, track_sign_properties_endpoint
        from auth import raide_api_headers

        # Headers required for Raide-API.
        raide_headers = raide_api_headers()

        # Fetch all the properties.
        sign_properties_endpoint = track_sign_properties_endpoint()
        result = self.httpclient.get(raide_url(sign_properties_endpoint),raide_headers)

        # Map each property to its info object.
        property_dict = {}
        for prop in result["properties"]:
            
            # Store the lowercase values for enum properties as they will be used for comparison.
            if prop["dataType"] == "ENUM":
                prop["enumList"]["lowerCaseValues"] = [v["value"].lower() for v in prop["enumList"]["enumValues"] if v["enabled"]]
            #self.track_sign_properties[prop["name"]] = prop
            property_dict[prop["name"]] = prop

        return property_dict