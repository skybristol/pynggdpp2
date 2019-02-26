from datetime import datetime
import dateutil.parser as dt_parser
import os
import json
import hashlib
from io import BytesIO
from io import StringIO
from urllib.parse import urlsplit

import boto3

import requests
from smart_open import smart_open
from bs4 import BeautifulSoup
import xmltodict
import pandas as pd
from geojson import Feature, Point, Polygon, FeatureCollection
from geojson import dumps as geojson_dumps
from geojson_utils import centroid
from gis_metadata.metadata_parser import get_metadata_parser
from gis_metadata.utils import get_supported_props


# Set variables
sb_catalog_path = "https://www.sciencebase.gov/catalog/items"
sb_vocab_path = "https://www.sciencebase.gov/vocab"
sb_default_max = "100"
sb_default_format = "json"
sb_default_props = "title,body,contacts,spatial,files,webLinks,facets,dates"
ndc_vocab_id = "5bf3f7bce4b00ce5fb627d57"
ndc_catalog_id = "4f4e4760e4b07f02db47dfb4"


class RedirectStdStreams(object):
    def __init__(self, stdout=None, stderr=None):
        self._stdout = stdout or sys.stdout
        self._stderr = stderr or sys.stderr

    def __enter__(self):
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        self.old_stdout.flush(); self.old_stderr.flush()
        sys.stdout, sys.stderr = self._stdout, self._stderr

    def __exit__(self, exc_type, exc_value, traceback):
        self._stdout.flush(); self._stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr


def ndc_collection_type_tag(tag_name, include_type=True):
    vocab_search_url = f'{sb_vocab_path}/' \
                       f'{ndc_vocab_id}/' \
                       f'terms?nodeType=term&format=json&name={tag_name}'
    r_vocab_search = requests.get(vocab_search_url).json()
    if len(r_vocab_search['list']) == 1:
        tag = {'name':r_vocab_search['list'][0]['name'],'scheme':r_vocab_search['list'][0]['scheme']}
        if include_type:
            tag['type'] = 'theme'
        return tag
    else:
        return None


def ndc_get_collections(parentId=ndc_catalog_id, fields=sb_default_props):
    nextLink = f'{sb_catalog_path}?' \
                           f'format={sb_default_format}&' \
                           f'max={sb_default_max}&' \
                           f'fields={fields}&' \
                           f'folderId={parentId}&' \
                           f"filter=tags%3D{ndc_collection_type_tag('ndc_collection',False)}"

    collectionItems = list()

    while nextLink is not None:
        r_ndc_collections = requests.get(nextLink).json()

        if "items" in r_ndc_collections.keys():
            collectionItems.extend(r_ndc_collections["items"])

        if "nextlink" in r_ndc_collections.keys():
            nextLink = r_ndc_collections["nextlink"]["url"]
        else:
            nextLink = None

    if len(collectionItems) == 0:
        collectionItems = None

    return collectionItems


def collection_meta(collection_id):
    r = requests.get(f"{sb_catalog_path}?"
                     f"id={collection_id}&"
                     f"format=json&"
                     f"fields={sb_default_props}"
                     ).json()

    collection_record = r["items"][0]

    collection_meta = dict()
    collection_meta["ndc_collection_id"] = collection_record["id"]
    collection_meta["ndc_collection_title"] = collection_record["title"]
    collection_meta["ndc_collection_link"] = collection_record["link"]["url"]
    collection_meta["ndc_collection_last_updated"] = next((d["dateString"] for d in collection_record["dates"]
                                                           if d["type"] == "lastUpdated"), None)
    collection_meta["ndc_collection_created"] = next((d["dateString"] for d in collection_record["dates"]
                                                           if d["type"] == "dateCreated"), None)
    collection_meta["ndc_collection_queried"] = datetime.utcnow().isoformat()

    if "contacts" in collection_record.keys():
        collection_meta["ndc_collection_owner"] = [c["name"] for c in collection_record["contacts"]
                               if "type" in c.keys() and c["type"] == "Data Owner"]
        if len(collection_meta["ndc_collection_owner"]) == 0:
            collection_meta["ndc_collection_improvements_needed"] = ["Need data owner contact in collection metadata"]
    else:
        collection_meta["ndc_collection_improvements_needed"] = ["Need contacts in collection metadata"]

    return collection_meta


def parse_waf(url):
    """
    Parses a Web Accessible Folder HTML response to build a data structure containing response header information and
    a listing of potentially harvestable XML file URLs. This sets up a list of harvestable items for a given collection,
    including the available date/time information that can be used to keep things in sync over time.

    :param url: URL to a Web Accessible Folder
    :return: Dictionary containing headers and url_list
    """
    try:
        r = requests.get(url)
    except:
        return None

    waf_package = dict()
    waf_package["url"] = r.url
    waf_package["headers"] = r.headers
    waf_package["url_list"] = list()

    soup = BeautifulSoup(r.content, "html.parser")

    if soup.find("pre"):
        processed_list = dict()

        for index, line in enumerate(soup.pre.children):
            if index > 1:
                try:
                    line_contents = line.get_text()
                except:
                    line_contents = list(filter(None, str(line).split(" ")))
                processed_list[index] = line_contents

        for k, v in processed_list.items():
            if k % 2 == 0:
                if v.split(".")[-1] == "xml":
                    item = dict()
                    item["file_name"] = v
                    item["file_url"] = f"{r.url}{v}"
                    item["file_date"] = dt_parser.parse(
                        f"{processed_list[k + 1][0]} {processed_list[k + 1][1]}").isoformat()
                    item["file_size"] = processed_list[k + 1][2].replace("\r\n", "")
                    waf_package["url_list"].append(item)

    elif soup.find("table"):
        for index, row in enumerate(soup.table.find_all("tr")):
            if index > 2:
                item = dict()
                columns = row.find_all("td")
                for i, column in enumerate(columns):
                    cell_text = column.get_text().strip()
                    if i == 1:
                        item["file_name"] = cell_text
                        item["file_url"] = f"{r.url}{cell_text}"
                    elif i == 2:
                        item["file_date"] = dt_parser.parse(cell_text).isoformat()
                    elif i == 3:
                        item["file_size"] = cell_text
                if "file_name" in item.keys() and item["file_name"].split(".")[-1] == "xml":
                    waf_package["url_list"].append(item)

    return waf_package


def introspect_nggdpp_xml(dict_data):
    introspection_meta = dict()
    top_keys = list(dict_data.keys())
    if len(top_keys) == 1:
        second_keys = list(dict_data[top_keys[0]].keys())
        if isinstance(dict_data[top_keys[0]][second_keys[-1]], list):
            introspection_meta['ndc_record_container_path'] = [top_keys[0], second_keys[-1]]
            introspection_meta['ndc_record_number'] = len(dict_data[top_keys[0]][second_keys[-1]])
            introspection_meta['ndc_field_names'] = list(dict_data[top_keys[0]][second_keys[-1]][0].keys())

    return introspection_meta


def nggdpp_xml_to_recordset(context):
    s3_object = get_s3_file(context['ndc_s3_file_key'])

    xml_bytes = BytesIO(s3_object['Body'].read())
    source_data = xmltodict.parse(xml_bytes.getvalue(), dict_constructor=dict)

    introspection_meta = introspect_nggdpp_xml(source_data)
    xml_tree_top = introspection_meta['ndc_record_container_path'][0]
    xml_tree_next = introspection_meta['ndc_record_container_path'][1]
    recordset = source_data[xml_tree_top][xml_tree_next]

    for item in recordset:
        for k, v in context.items():
            item.update({k:v})
        for k, v in introspection_meta.items():
            item.update({k:v})

    data_package = {
        "errors": None,
        "recordset": recordset
    }

    return data_package


def nggdpp_text_to_recordset(context):
    with smart_open(f's3://ndc/{context["ndc_s3_file_key"]}', 'r', host=os.environ['AWS_HOST_S3']) as f:

        delimiter = "|"
        quotechar = ''
        quoting = csv.QUOTE_NONE
        first_line = f.readline()
        for possibility in ["|", ",", "\t"]:
            if possibility in first_line:
                delimiter = possibility
                break
        if '"' in first_line:
            quotechar = '"'
            quoting = csv.QUOTE_MINIMAL

        f.seek(0)

        devnull = StringIO()
        with RedirectStdStreams(stdout=devnull, stderr=devnull):
            df_file = pd.read_csv(f,
                                  sep=delimiter,
                                  quotechar=quotechar,
                                  error_bad_lines=False,
                                  warn_bad_lines=True,
                                  quoting=quoting)
        errors = devnull.getvalue()

    if df_file is not None:
        df_file = df_file.dropna(how="all")
        df_file.drop(df_file.columns[df_file.columns.str.contains('unnamed', case=False)], axis=1)
        json_file = df_file.to_json(orient="records")
        recordset = json.loads(json_file)

        for item in recordset:
            for k, v in context.items():
                item.update({k: v})
    else:
        recordset = None

    if len(errors) == 0:
        errors = None

    data_package = {
        "errors": errors,
        "recordset": recordset
    }

    return data_package


def build_ndc_metadata(context):
    context_meta = {}
    for section, content in context.items():
        for k, v in content.items():
            if k.split("_")[0] != "ndc":
                k = f"ndc_{k}"
            context_meta.update({k: v})

    if "ndc_pathOnDisk" in context_meta.keys():
        context_meta["ndc_s3_file_key"] = f"{context_meta['ndc_pathOnDisk']}/{context_meta['ndc_name']}"
    elif "ndc_file_url" in context_meta.keys():
        context_meta["ndc_s3_file_key"] = url_to_s3_key(context_meta["ndc_file_url"])

    context_meta["ndc_date_record_created"] = datetime.utcnow().isoformat()
    return context_meta


def nggdpp_recordset_to_feature_collection(recordset):
    feature_list = []

    for record in recordset:
        p = {k.lower(): v for k, v in record.items()}
        p["ndc_processing_errors"] = list()
        p["ndc_processing_errors_number"] = 0

        if "coordinates" not in p.keys():
            if ("latitude" in p.keys() and "longitude" in p.keys()):
                p["coordinates"] = f'{p["longitude"]},{p["latitude"]}'

        g = Point(None)
        if "coordinates" in p.keys() and p["coordinates"] is not None:
            try:
                if "," in p["coordinates"]:
                    try:
                        g = Point((float(p["coordinates"].split(',')[0]), float(p["coordinates"].split(',')[1])))
                    except:
                        pass
            except Exception as e:
                p["ndc_processing_errors"].append(
                    {
                        "error": e,
                        "info": f"{str(p['coordinates'])}; kept empty geometry"
                    }
                )
                p["ndc_processing_errors"]+=1

        feature_list.append(Feature(geometry=g, properties=p))

    return FeatureCollection(feature_list)


def feature_from_metadata(collection_meta, link_meta):
    meta_doc = get_s3_file(link_meta["aws_s3_key"]).getvalue()

    # Create structure for properties starting with infused collection metadata
    p = collection_meta

    # Add in link specific metadata with the ndc_ prefix
    for k, v in link_meta.items():
        p[f"ndc_{k}"] = v

    p["ndc_record_date"] = datetime.utcnow().isoformat()

    # Parse the metadata using the gis_metadata tools
    parsed_metadata = get_metadata_parser(meta_doc)

    # Add any and all properties that aren't blank (ref. gis_metadata.utils.get_supported_props())
    #for prop in get_supported_props():
    for prop in get_supported_props():
        v = parsed_metadata.__getattribute__(prop)
        if len(v) > 0:
            p[prop.lower()] = v

    # Process geometry if a bbox exists
    p['coordinates_point'] = {}

    if len(parsed_metadata.bounding_box) > 0:
        # Pull out bounding box elements
        east = float(parsed_metadata.bounding_box["east"])
        west = float(parsed_metadata.bounding_box["west"])
        south = float(parsed_metadata.bounding_box["south"])
        north = float(parsed_metadata.bounding_box["north"])

        # Record a couple processable forms of the BBOX for later convenience
        p['coordinates_geojson'] = [[west, north], [east, north], [east, south], [west, south]]
        p['coordinates_wkt'] = [[(west, north), (east, north), (east, south), (west, south), (west, north)]]

        # Determine the best way to provide point coordinates and record the method
        if east == west and south == north:
            p['coordinates_point']['coordinates'] = f"{east},{south}"
            p['coordinates_point']['method'] = "bbox corner"
        else:
            c = centroid(Polygon(p['coordinates_wkt']))['coordinates']
            p['coordinates_point']['coordinates'] = f"{c[0]},{c[1]}"
            p['coordinates_point']['method'] = "bbox centroid"
    else:
        p['coordinates_point']['coordinates'] = None
        p['coordinates_point']['method'] = "no processable geometry"

    # Generate the point geometry
    g = build_point_geometry(p["coordinates_point"]["coordinates"])

    # Build the GeoJSON feature from the geometry with its properties
    f = build_ndc_feature(g, p)

    # Convert the geojson object to a standard dict
    f = json.loads(geojson_dumps(f))

    return f


def url_to_s3_key(url):
    parsed_url = urlsplit(url)
    return f"{parsed_url.netloc}{parsed_url.path}"


def s3_json_to_dict(aws_key, bucket_name='ndc'):
    try:
        with smart_open(f's3://{bucket_name}/{aws_key}', 'r', host=os.environ['AWS_HOST_S3']) as f:
            data_package = json.loads(f.read())

        return data_package

    except:
        return None


def get_s3_file(key, bucket_name='ndc'):
    s3_client = boto3.client('s3', endpoint_url=os.environ['AWS_HOST_S3'])
    bucket_object = s3_client.get_object(Bucket=bucket_name, Key=key)
    file_bytes = BytesIO(bucket_object['Body'].read())

    return file_bytes


def remove_s3_object(key, bucket_name='ndc'):
    s3_resource = boto3.resource('s3', endpoint_url=os.environ['AWS_HOST_S3'])
    response = s3_resource.Object(bucket_name, key).delete()

    return response


def transfer_file_to_s3(source_url, key_name=None):
    s3 = boto3.resource('s3',
                        endpoint_url=os.environ['AWS_HOST_S3'])

    if key_name is None:
        key_name = url_to_s3_key(source_url)

    bucket_object = s3.Object('ndc', key_name)

    file_object = requests.get(source_url).content

    bucket_response = bucket_object.put(Body=file_object)

    return bucket_response


def put_file_to_s3(source_data, key_name, bucket_name='ndc'):
    s3 = boto3.resource('s3',
                        endpoint_url=os.environ['AWS_HOST_S3'])

    bucket_object = s3.Object(bucket_name, key_name)

    bucket_response = bucket_object.put(Body=json.dumps(source_data))

    return bucket_response


def q_url(QueueName):
    sqs = boto3.client('sqs',
                       endpoint_url=os.environ['AWS_HOST_SQS'])

    try:
        return sqs.get_queue_url(QueueName=QueueName)["QueueUrl"]
    except:
        return None


def get_message(QueueName):
    sqs = boto3.client('sqs',
                       endpoint_url=os.environ['AWS_HOST_SQS'])

    try:
        QueueUrl = sqs.get_queue_url(QueueName=QueueName)["QueueUrl"]
    except:
        return None

    response = sqs.receive_message(
        QueueUrl=QueueUrl,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )

    if "Messages" not in response.keys() or len(response['Messages']) == 0:
        return None

    message = {
        "ReceiptHandle": response['Messages'][0]['ReceiptHandle'],
        "Body": json.loads(response['Messages'][0]['Body'])
    }

    return message


def post_message(QueueName, identifier, body):
    sqs = boto3.client('sqs',
                       endpoint_url=os.environ['AWS_HOST_SQS'])

    try:
        QueueUrl = sqs.get_queue_url(QueueName=QueueName)["QueueUrl"]
    except:
        return None

    try:
        response = sqs.send_message(
            QueueUrl=QueueUrl,
            MessageAttributes={
                'identifier': {
                    'DataType': 'String',
                    'StringValue': identifier
                }
            },
            MessageBody=(
                json.dumps(body)
            )
        )
    except Exception as e:
        aws_key = f"sqs_stash/{hashlib.md5(identifier.encode('utf-8')).hexdigest()}"

        message_body = {
            "error": str(e),
            "aws_key": aws_key,
            "response": put_file_to_s3(body, aws_key)
        }

        response = sqs.send_message(
            QueueUrl=QueueUrl,
            MessageAttributes={
                'identifier': {
                    'DataType': 'String',
                    'StringValue': identifier
                }
            },
            MessageBody=(
                json.dumps(message_body)
            )
        )

    return response['MessageId']


def delete_message(QueueName, ReceiptHandle):
    sqs = boto3.client('sqs',
                       endpoint_url=os.environ['AWS_HOST_SQS'])

    try:
        QueueUrl = sqs.get_queue_url(QueueName=QueueName)["QueueUrl"]
    except:
        return None

    sqs.delete_message(
        QueueUrl=QueueUrl,
        ReceiptHandle=ReceiptHandle
    )

    return ReceiptHandle


def build_point_geometry(coordinates):
    try:
        pointGeometry = Point((float(coordinates.split(',')[0]), float(coordinates.split(',')[1])))
    except:
        pointGeometry = Point(None)
    return pointGeometry


def build_ndc_feature(geom, props):
    ndcFeature = Feature(geometry=geom, properties=props)
    return ndcFeature


def build_ndc_feature_collection(feature_list):
    return FeatureCollection(feature_list)

