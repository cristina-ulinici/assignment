## Prerequisites

I used python 3.11.0.

## Usage

Install the requirements:
```
pip install -r /code/requirements.txt
```
Run the server:
```
uvicorn app.main:app
```

Using Postman for example make a POST request to `localhost:8000/enrich/` with a `file` containing a csv file with data to be enriched. This request should return the enriched data in a json format. An example of input file can be found in `test/data/input_dataset.csv`

## Testing

```
pytest ./test/test.py
```
