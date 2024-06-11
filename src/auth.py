import os
import base64

user = os.getenv("RAIDE_USER")
password = os.getenv("RAIDE_PASS")

def token():

    if user is None or password is None:
        raise ValueError("Raide-API tunnuksia ei löytynyt.")

    return base64.b64encode(f"{user}:{password}".encode('utf-8')).decode('ascii')


def raide_api_headers():

    headers= {
        'Accept-Encoding': 'gzip',
        'Content-Type': 'application/json; charset=UTF-8',
        'Authorization':'Basic {}'.format(token())
    }

    return headers