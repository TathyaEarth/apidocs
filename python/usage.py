import json
from enum import Enum
import requests
import logging
from datetime import datetime

logging.basicConfig(encoding='utf-8', level=logging.INFO)

_token = None
_username = None
_password = None
hostname = "api.tathya.earth"



class Product(Enum):
    ALL = 'ALL'
    COAL = 'COAL'
    HOTMETAL = 'HOTMETAL'
    IRONORE = 'IRONORE'
    MINING = 'MINING'

def batch(datas, chunksize):
    """Split list into the chucks
    """
    for i in range(0, len(datas), chunksize):
        yield datas[i:i + chunksize]

def set_credentials(username, password):
    global _username, _password
    _username = username
    _password = password

class AuthException(Exception):
    pass

def get_token():
    """
        Generates auth token object to be used for authenticated enpoints
        Ref URL: https://tathya.earth/docs/#operation/getToken
    """
    assert _username is not None, "Set credentials before calling get_token"
    global _token
    if _token is None or datetime.fromisoformat(_token.get("expires_at")) < datetime.now():
        req = requests.post(f'https://{hostname}/api/v2/token', json={"username": _username, "password": _password})
        if req.ok:
            _token = req.json()
        else:
            raise AuthException(f"Authentication Failed. Output: {json.dumps(req.json())}")


def get_request(request_method, endpoint, data={}, params={}):
    assert callable(request_method), "Request method needs to be a function requests.get or requests.post"
    res = request_method(f'https://{hostname}{endpoint}', params=params, json=data, headers={'Authorization': f'Bearer {_token.get("token")}'})
    logging.info(f"Requesting the URL: {res.url}")
    return res


def get_subscribed_indexes(product=Product.ALL):
    """
    Get all product subscribed indexes
    Ref URL: https://tathya.earth/docs/#operation/getSubscriptions
    """
    product = product.value if isinstance(product, Product) else product
    res = get_request(requests.get, f'/api/v2/subscriptions/{product}')
    data = res.json()
    subscribed_indices = []
    for indices in data.get('data', []):
        subscribed_indices += list(map(lambda x: x, indices.get('indexes')))
    logging.info("Subscribed indices: %s", str(subscribed_indices))
    return subscribed_indices


def get_indexes_data(product=Product.HOTMETAL, start_date=None, end_date=None, identifiers=[], batch_size=5):
    """
    Get index identifiers data in a batched manner
    Ref URL: https://tathya.earth/docs/#operation/getIndexData
    """
    params = dict()
    if start_date is not None:
        params['start_date'] = start_date
    if end_date is not None:
        params['end_date'] = end_date
    params['product'] = product.value if isinstance(product, Product) else product
    index_data = []
    # Batching the full identifier list
    for identifiers_batch in batch(identifiers, batch_size):
        params['identifier'] = ','.join(identifiers_batch)
        res = get_request(requests.get, f'/api/v2/index/data/query', params=params)
        data = res.json()
        index_data += data.get('results')
    return {"results": index_data, "success": True}