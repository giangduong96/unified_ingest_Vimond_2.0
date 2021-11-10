import json

import boto3
import urllib
import preprocessor
import vcc_wrapper as vcc

import os


def get_s3_file(bucket_name, key):
    s3 = boto3.resource('s3')
    try:
        object = s3.Object(bucket_name, key)
        res = object.get()
        file = res["Body"]
    except Exception as e:
        raise e
    return file

def create_db_record(db_dict, ingest_endpoint):
    region = os.environ['region']
    dynamodb_table = os.environ['table']
    if (not db_dict):
        return
    if (ingest_endpoint):
        db_dict["Ingest Endpoint"] = ingest_endpoint
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(dynamodb_table)
    try:
        res = table.put_item(
            Item=db_dict
        )
        print("Log {dynamodb_table} put response: {res}")
    except Exception as e:
        raise e

def call_sfn(sfn_arn, payload):
    try:
        client = boto3.client('stepfunctions')
        response = client.start_execution(
            stateMachineArn=str(sfn_arn),
            input=json.dumps(payload)
        )
        print("Successfully invoked sfn {sfn_arn}")
        print(response)
    except Exception as e:
        print(str(e))
        print("Error: sfn={sfn_arn} payload={payload}")
    return

def lambda_handler(event, context):
    print(event)
    # input file path, no white spaces, only .xls extentions
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    key = urllib.parse.unquote_plus(key)
    provider_bucket = key.split('/')[0]
    print(bucket_name, key)

    # download excel sheet
    file = get_s3_file(bucket_name, key)

    # parse excel sheet
    p = preprocessor.Preprocessor()

    arr = p.excel_file_to_array(file)

    # convert 2d array of excel rows into list[dict] where dict is the parsed excel row
    #provider = os.environ["provider_name_from_s3_folder"]  ## TODO: implement method to use "key" variable to get "provider"
    if provider_bucket == "walterpresents":
        provider = "walterpresents"
    else:
        provider = "davincikids"
    batch_parsed_excel = []
    db_dict = {}
    for row in arr:
        db_dict = p.parse_row_into_db_dict(row,provider_bucket)
        db_dict["Provider"] = provider
        batch_parsed_excel.append(db_dict)

    print(batch_parsed_excel)

    # check if season already exists
    # if not, create else pass
    url = os.environ["vcc_url"]
    tenant = os.environ["vcc_tenant"]
    if provider_bucket == "walterpresents":
        publisher_id = 2454
    else:
        publisher_id = 2114
    #publisher_id = os.environ["vcc_publisher_id"]  ## TODO: implement method to get "publisher_id" from "provider" in vcc
    user = os.environ["vcc_user"]
    password = os.environ["vcc_password"]
    v = vcc.vcc_wrapper(url, tenant, publisher_id, user, password)
    if provider_bucket == "walterpresents":
        root_id = 33815
    else:
        root_id = 20531
    #root_id = os.environ["vcc_series_root_id"]  ## TODO: implement method to get "root_id" from "publisher_id" in vcc
    series_name = db_dict.get("Series Name")
    season_name = "Season " + str(db_dict.get("Season Number"))
    season_category_id = v.create_vcc_categories(root_id, series_name, season_name)

    # check for feed ingest, if not exist create
    ingest_endpoint = v.get_feed_ingest_endpoint(season_category_id, publisher_id)  # get ingest endpoint for that season
    if(not ingest_endpoint):
        ingest_name = series_name + " " + season_name
        ingest_endpoint = v.create_vcc_push_feed_ingest(season_category_id, ingest_name, publisher_id)

    # put rows into db
    for row in batch_parsed_excel:
        create_db_record(row, ingest_endpoint)

    # start batched ingest process
    path = provider + "/Series/" + series_name + "/" + season_name # path to asset folder, ex: "WalterPresents/Series/Series_Name/Season 1"
    wp_sfn_arn = os.environ["wp_sfn_arn"]
    for row in batch_parsed_excel:
        payload = {
            "provider_external_id": row["Provider External ID"],
            "provider": provider,
            "bucket_name": bucket_name,
            "bucket_path": path
        }
        call_sfn(wp_sfn_arn, payload)

    ret = {
        "provider_external_id": db_dict["Provider External ID"],
        "provider": provider,
        "bucket_name": bucket_name,
        "bucket_path": path
    }

    return ret
