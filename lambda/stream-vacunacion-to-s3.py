import boto3
import requests


def lambda_handler(event, context):
    url = "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/datos_nomivac_covid19.zip"

    session = requests.Session()
    response = session.get(url, stream=True)
    s3_bucket = "covidbucket-s3"
    s3_file_path = "datos_nomivac_covid19.zip"
    s3 = boto3.client("s3")
    with response as part:
        part.raw.decode_content = True
        conf = boto3.s3.transfer.TransferConfig(
            multipart_threshold=10000, max_concurrency=4
        )
        s3.upload_fileobj(part.raw, s3_bucket, s3_file_path, Config=conf)