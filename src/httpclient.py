import requests

class HttpClient:

    """
    Simple httpclient with session.
    """

    def __init__(self):
        self.session = requests.Session()


    def get(self,url: str, headers = {}):

        result = self.session.get(url,headers=headers)
        result.raise_for_status()

        return result.json()