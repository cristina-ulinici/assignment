import io
import csv
import requests


GLEIF_URL = 'https://api.gleif.org/api/v1/lei-records?filter[lei]={0}'


class GleifNotReachable(Exception):
    """ Exception raised when gleif is not available due to connection errors """
    def __init__(self, error:str = 'Unknown'):
        self.message = f'Gleif service not reachable: {error}'
        super().__init__(self.message)


class RateZero(Exception):
    """ Exception raied when trying to compute transactions_costs with 0 rate """
    def __init__(self, lei: str):
        self.message = f'Could not compute transactions_costs for {lei}; rate is 0'
        super().__init__(self.message)


class Entry:
    def __init__(self, entry: dict, processor: 'DataProcessor'):
        self.entry = entry
        self.processor = processor
        lei = entry.get('lei')
        self.gleif_attributes = self._get_gleif_attributes(lei)

    def enrich(self):
        if not self.gleif_attributes:
            self._enrich_with_none()
            return self.entry
        self._add_legal_name_and_bic()
        try:
            self._add_transactions_costs()
        except RateZero as e:
            self.processor.report_error(e)

        return self.entry

    def _get_gleif_attributes(self, lei: str):
        try:
            data = self._get_glief_data(lei)
        except GleifNotReachable as e:
            self.processor.report_error(e)
            return None
        return data.get('data', [{}])[0].get('attributes')

    def _get_glief_data(self, lei: str):
        try:
            r = requests.get(url = GLEIF_URL.format(lei))
            r.raise_for_status()
        except Exception as e:
            raise GleifNotReachable(type(e).__name__)
        else:
            data = r.json()
        return data

    def _enrich_with_none(self):
        self.entry['legal_name'] = None
        self.entry['bic'] = []
        self.entry['transactions_costs'] = None

    def _add_legal_name_and_bic(self):
        self.entry['legal_name'] = self.gleif_attributes.get('entity', {}).get('legalName', {}).get('name')
        self.entry['bic'] = self.gleif_attributes.get('bic')

    def _add_transactions_costs(self):
        self.entry['transactions_costs'] = None
        legal_address_country = self.gleif_attributes.get('entity', {}).get('legalAddress', {}).get('country')
        notional = self.entry.get('notional')
        rate = self.entry.get('rate')

        if not notional or not rate:
            return

        notional, rate = float(notional), float(rate)

        if legal_address_country == 'GB':
            self.entry['transactions_costs'] = str(notional * rate - notional)
        elif legal_address_country == 'NL':
            if rate == 0:
                raise RateZero(lei=self.entry.get('lei'))
            self.entry['transactions_costs'] = str(abs(notional * (1 / rate) - notional))


class DataProcessor:
    def __init__(self, content: bytes):
        self.content = content
        self.data = []
        self.errors = []

    def get_enriched_data(self):
        for entry in self._get_entries_from_file():
            self.data.append(Entry(entry, self).enrich())
        return self.data

    def get_errors(self):
        return [str(e) for e in self.errors]

    def report_error(self, e: Exception):
        self.errors.append(e)

    def _get_entries_from_file(self):
        content = self.content.decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(content), delimiter=',')

        for entry in csv_reader:
            yield entry


if __name__ == '__main__':
    import os.path

    wd = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(wd, "../test/data/input_dataset.csv")
    content = open(path, 'rb').read()

    data = DataProcessor(content).get_enriched_data()
    print(data)
