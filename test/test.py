import json
import copy
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app
from app.enrichment import Entry, DataProcessor, GleifNotReachable


client = TestClient(app)


def get_mock_gleif_data():
    f = open('test/data/gleif_sample.json')
    return json.load(f)


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World"}


@patch('app.enrichment.Entry._get_glief_data', return_value=get_mock_gleif_data())
def test_enrich_ok(get_gleif_data_mock):
    file = open('test/data/input_dataset.csv', 'rb')
    response = client.post("/enrich/", files={"file": ("filename", file)})
    assert response.status_code == 200
    json_response = response.json()
    assert json_response['message'] == 'OK'
    assert len(json_response['data']) == 20


@patch('app.enrichment.Entry._get_glief_data', return_value=get_mock_gleif_data())
def test_enrich_nok(get_gleif_data_mock):
    file = open('test/data/input_dataset_rate_0.csv', 'rb')
    response = client.post("/enrich/", files={"file": ("filename", file)})
    assert response.status_code == 200
    json_response = response.json()
    assert len(json_response['data']) == 2
    error_msg = 'Could not compute transactions_costs for BFXS5XCH7N0Y05NIXW11; rate is 0'
    assert json_response['message'] == f"Some entries could not be enriched: ['{error_msg}']"


ENTRY_SAMPLE = {
    'transaction_uti': '1030291281MARKITWIRE0000000000000112874138',
    'isin': 'EZ9724VTXK48',
    'notional': '763000.0',
    'notional_currency': 'GBP',
    'transaction_type': 'Sell',
    'transaction_datetime': '2020-11-25T15:06:22Z',
    'rate': '0.0070956000',
    'lei': 'XKZZ2JZF41MRHTR1V493'
}

ENRICHED_ENTRY = {
    'transaction_uti': '1030291281MARKITWIRE0000000000000112874138',
    'isin': 'EZ9724VTXK48',
    'notional': '763000.0',
    'notional_currency': 'GBP',
    'transaction_type': 'Sell',
    'transaction_datetime': '2020-11-25T15:06:22Z',
    'rate': '0.0070956000',
    'lei': 'XKZZ2JZF41MRHTR1V493',
    'bic': ['STDANL21XXX'],
    'legal_name': 'Stichting Pensioenfonds Alliance',
    'transactions_costs': '106768427.92716615'
}

ENRICHED_WITH_NONE = {
    'transaction_uti': '1030291281MARKITWIRE0000000000000112874138',
    'isin': 'EZ9724VTXK48',
    'notional': '763000.0',
    'notional_currency': 'GBP',
    'transaction_type': 'Sell',
    'transaction_datetime': '2020-11-25T15:06:22Z',
    'rate': '0.0070956000',
    'lei': 'XKZZ2JZF41MRHTR1V493',
    'bic': [],
    'legal_name': None,
    'transactions_costs': None
}


@patch('app.enrichment.Entry._get_glief_data', return_value=get_mock_gleif_data())
def test_entry_enrich(get_gleif_data_mock):
    entry = copy.deepcopy(ENTRY_SAMPLE)
    enriched_entry = Entry(entry, DataProcessor(b'')).enrich()

    assert enriched_entry == ENRICHED_ENTRY


@patch('app.enrichment.Entry._get_glief_data', side_effect=GleifNotReachable())
def test_entry_enrich_no_gleif_data(get_gleif_data_mock):
    entry = copy.deepcopy(ENTRY_SAMPLE)
    enriched_entry = Entry(entry, DataProcessor(b'')).enrich()

    assert enriched_entry == ENRICHED_WITH_NONE
