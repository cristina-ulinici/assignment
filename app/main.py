from fastapi import FastAPI, UploadFile, HTTPException

from app.enrichment import DataProcessor


app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/enrich/")
async def enrich(file: UploadFile):
    """
    Recieves csv file with entries to be enriched and returns a json data structure
    with the enriched data. json was chosen because it is an universal  and
    straight-forward format that can easily be integrated in other systems.
    """
    b_content = file.file.read()
    data_processor = DataProcessor(b_content)
    data = data_processor.get_enriched_data()
    errors = data_processor.get_errors()

    if not errors:
        message = 'OK'
    else:
        message = f'Some entries could not be enriched: {errors}'

    return {'message': message, 'data': data}
