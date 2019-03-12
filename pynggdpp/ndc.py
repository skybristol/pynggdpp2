from datetime import datetime
import dateutil.parser as dt_parser
import os
import json
import csv
import sys
import hashlib
from io import BytesIO
from urllib.parse import urlsplit

import boto3

import requests
from bs4 import BeautifulSoup
import xmltodict
from geojson import Feature, Point, Polygon, FeatureCollection
from geojson import dumps as geojson_dumps
from geojson_utils import centroid
from gis_metadata.metadata_parser import get_metadata_parser
from gis_metadata.utils import get_supported_props
from elasticsearch import Elasticsearch


# Set variables
sb_catalog_path = "https://www.sciencebase.gov/catalog/items"
sb_vocab_path = "https://www.sciencebase.gov/vocab"
sb_default_max = "100"
sb_default_format = "json"
sb_default_props = "title,body,contacts,spatial,files,webLinks,facets,dates"
ndc_vocab_id = "5bf3f7bce4b00ce5fb627d57"
ndc_catalog_id = "4f4e4760e4b07f02db47dfb4"
acceptable_content_types = [
    'text/plain',
    'text/plain; charset=ISO-8859-1',
    'application/xml',
    'text/csv',
    'text/plain; charset=windows-1252'
]
es_server_config = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}



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


def get_collection_record(collection_id):
    r = requests.get(f"{sb_catalog_path}?"
                     f"id={collection_id}&"
                     f"format=json&"
                     f"fields={sb_default_props}"
                     ).json()

    if len(r["items"]) == 0:
        return None
    else:
        return r["items"][0]


def collection_meta(collection_id=None, collection_record=None):
    if collection_record is None:
        collection_record = get_collection_record(collection_id)

    collection_meta = dict()
    collection_meta["ndc_collection_id"] = collection_record["id"]
    collection_meta["ndc_collection_title"] = collection_record["title"]
    collection_meta["ndc_collection_link"] = collection_record["link"]["url"]
    collection_meta["ndc_collection_last_updated"] = next((d["dateString"] for d in collection_record["dates"]
                                                           if d["type"] == "lastUpdated"), None)
    collection_meta["ndc_collection_created"] = next((d["dateString"] for d in collection_record["dates"]
                                                           if d["type"] == "dateCreated"), None)
    collection_meta["ndc_collection_queried"] = datetime.utcnow().isoformat()

    improvement_messages = list()

    if "body" in collection_record.keys():
        collection_meta["ndc_collection_abstract"] = collection_record["body"]
    else:
        improvement_messages.append("Need abstract for collection")

    if "contacts" in collection_record.keys():
        collection_meta["ndc_collection_owner"] = [c["name"] for c in collection_record["contacts"]
                               if "type" in c.keys() and c["type"] == "Data Owner"]
        if len(collection_meta["ndc_collection_owner"]) == 0:
            improvement_messages.append("Need data owner contact in collection metadata")
    else:
        improvement_messages.append("Need contacts in collection metadata")

    if len(improvement_messages) > 0:
        collection_meta["ndc_collection_improvements_needed"] = improvement_messages

    return collection_meta


def build_processable_routes(collection_id=None, collection_record=None):
    if collection_record is None:
        collection_record = get_collection_record(collection_id)

    collection_packet = dict()
    collection_packet["collection_meta"] = collection_meta(collection_record=collection_record)

    if "files" in collection_record.keys():
        potentially_processable_files = [f for f in collection_record["files"] if f["name"] != "metadata.xml" and
                              "contentType" in f.keys() and
                              f["contentType"] in acceptable_content_types]
        if len(potentially_processable_files) > 0:
            collection_packet["collection_files"] = potentially_processable_files

    if "webLinks" in collection_record.keys() and \
            next((l for l in collection_record["webLinks"] if "type" in l.keys() and
                                                              l["type"] == "WAF"), None) is not None:
        potentially_processable_weblinks = [l for l in collection_record["webLinks"]
                                                 if "type" in l.keys() and l["type"] == "WAF"]
        if len(potentially_processable_weblinks) > 0:
            collection_packet["collection_links"] = potentially_processable_weblinks

    if "collection_files" in collection_packet.keys() or "collection_links" in collection_packet.keys():
        collection_packet["process_queue"] = "processable_collections"
    else:
        collection_packet["process_queue"] = "dlq_collections"

    return collection_packet


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


def nggdpp_xml_to_recordset(file_key, context):
    s3_object = get_s3_file(file_key, bucket_name="ndc-collection-files")

    source_data = xmltodict.parse(s3_object.getvalue(), dict_constructor=dict)

    introspection_meta = introspect_nggdpp_xml(source_data)
    xml_tree_top = introspection_meta['ndc_record_container_path'][0]
    xml_tree_next = introspection_meta['ndc_record_container_path'][1]
    recordset = source_data[xml_tree_top][xml_tree_next]
    recordset = [{k.lower():v for k,v in i.items()} for i in recordset]

    for item in recordset:
        item.update(introspect_coordinates(item))
        for k, v in context.items():
            item.update({k:v})
        for k, v in introspection_meta.items():
            item.update({k:v})

    data_package = {
        "errors": None,
        "recordset": recordset
    }

    return data_package


def nggdpp_text_to_recordset(file_key, content_meta, bucket_name="ndc-collection-files"):
    csv_lines = get_s3_file(file_key, bucket_name=bucket_name, return_type='lines')

    if isinstance(csv_lines, str):
        errors = [csv_lines]
        s3 = s3_client()
        bucket_object = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_lines = bucket_object['Body'].read().splitlines(True)
        recordset = list()
        for line in csv_lines:
            try:
                line_content = line.decode('utf-8').rstrip()
            except UnicodeDecodeError:
                line_content = line.decode('latin-1').rstrip()

            if "dialect" not in locals():
                dialect = csv.Sniffer().sniff(line_content, ['|', ',', ';', '\t'])
            line_data = line_content.split(dialect.delimiter)
            if "headers" not in locals():
                headers = line_data
            else:
                if len(line_data) == len(headers):
                    record_dict = dict()
                    for index, item in enumerate(line_data):
                        record_dict[headers[index]] = item
                    recordset.append(record_dict)
                else:
                    errors.append(str(line_data))
    else:
        errors = None
        reader = csv.DictReader(csv_lines)
        recordset = json.loads(json.dumps(list(reader)))

    recordset = [{k.lower(): v for k, v in i.items()} for i in recordset]
    for item in recordset:
        item.update(introspect_coordinates(item))
        for k, v in content_meta.items():
            item.update({k: v})

    data_package = {
        "errors": errors,
        "recordset": recordset
    }

    return data_package


def introspect_coordinates(item):
    item["ndc_processing_errors"] = list()
    item["ndc_processing_errors_number"] = 0

    if "coordinates" not in item.keys():
        if ("latitude" in item.keys() and "longitude" in item.keys()):
            item["coordinates"] = f'{item["longitude"]},{item["latitude"]}'

    if "coordinates" in item.keys() and item["coordinates"] is not None:
        try:
            if "," in item["coordinates"]:
                try:
                    item["ndc_location"] = \
                        Point((float(item["coordinates"].split(',')[0]), float(item["coordinates"].split(',')[1])))
                    item["ndc_geopoint"] = {
                        "lon": float(item["coordinates"].split(',')[0]),
                        "lat": float(item["coordinates"].split(',')[1])
                    }
                except:
                    pass
        except Exception as e:
            item["ndc_processing_errors"].append(
                {
                    "error": e,
                    "info": f"{str(item['coordinates'])}; kept empty geometry"
                }
            )
            item["ndc_processing_errors_number"]+=1
    else:
        item["ndc_processing_errors"].append(
            {
                "error": "Null Coordinates",
                "info": "Count not determine location from data"
            }
        )
        item["ndc_processing_errors_number"] += 1

    return item



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
                p["ndc_processing_errors_number"]+=1

        feature_list.append(Feature(geometry=g, properties=p))

    return FeatureCollection(feature_list)


def feature_from_metadata(collection_meta, link_meta):
    meta_doc = get_s3_file(link_meta["key_name"]).getvalue()

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

        p['coordinates_point']['coordinates'] = f"{east},{south}"
        p['coordinates_point']['method'] = "bbox corner"
        p['ndc_geopoint'] = f"{east},{south}"
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


def s3_client():
    return boto3.client('s3', endpoint_url=os.environ['AWS_HOST_S3'])


def url_to_s3_key(url):
    parsed_url = urlsplit(url)
    return f"{parsed_url.netloc}{parsed_url.path}"


def get_s3_file(key, bucket_name='ndc-collection-files', return_type='bytes'):
    s3 = s3_client()
    bucket_object = s3.get_object(Bucket=bucket_name, Key=key)

    if return_type == "raw":
        return bucket_object
    elif return_type == "bytes":
        return BytesIO(bucket_object['Body'].read())
    elif return_type == "dict":
        return json.loads(BytesIO(bucket_object['Body'].read()).read())
    elif return_type == "lines":
        try:
            return bucket_object['Body'].read().decode('utf-8').splitlines(True)
        except UnicodeDecodeError:
            return "File encoding problem encountered"


def remove_s3_object(key, bucket_name='ndc-collection-files'):
    s3_resource = boto3.resource('s3', endpoint_url=os.environ['AWS_HOST_S3'])
    response = s3_resource.Object(bucket_name, key).delete()

    return response


def transfer_file_to_s3(source_url, bucket_name="ndc-collection-files", key_name=None):
    s3 = boto3.resource('s3',
                        endpoint_url=os.environ['AWS_HOST_S3'])

    if key_name is None:
        key_name = url_to_s3_key(source_url)

    bucket_object = s3.Object(bucket_name, key_name)

    file_object = requests.get(source_url).content

    bucket_response = bucket_object.put(Body=file_object)

    return {
        "key_name": key_name,
        "source_url": source_url,
        "bucket_response": bucket_response
    }


def put_file_to_s3(source_data, key_name, bucket_name='ndc'):
    s3 = boto3.resource('s3',
                        endpoint_url=os.environ['AWS_HOST_S3'])

    bucket_object = s3.Object(bucket_name, key_name)

    bucket_response = bucket_object.put(Body=json.dumps(source_data))

    return bucket_response


def check_s3_file(key_name, bucket_name):
    s3 = boto3.resource('s3',
                        endpoint_url=os.environ["AWS_HOST_S3"])

    try:
        s3.Object(bucket_name, key_name).load()
    except:
        return False
    else:
        return True


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


def elastic_client():
    return Elasticsearch(hosts=[os.environ["AWS_HOST_Elasticsearch"]])


def create_es_index(index_name):
    responses = list()
    es = elastic_client()
    body = es_server_config
    if es.indices.exists(index_name):
        responses.append(es.indices.delete(index=index_name))
    responses.append(es.indices.create(index=index_name, body=body))
    return responses


def bulk_build_es_index(index_name, bulk_data):
    es = elastic_client()
    if not es.indices.exists(index_name):
        create_es_index(index_name)
    r = es.bulk(index=index_name, body=bulk_data, refresh=True)
    return r


def index_record_list(index_name, record_list):
    es = elastic_client()
    if not es.indices.exists(index_name):
        create_es_index(index_name)
    for record in record_list:
        es.index(index=index_name, doc_type='ndc_collection_item', body=record)
    return len(record_list)


def map_index(index_name):
    es = elastic_client()
    mapping = {
        "ndc_collection_item": {
            "properties": {
                "ndc_geopoint": {
                    "type": "geo_point"
                }
            }
        }
    }
    r = es.indices.put_mapping(index=index_name, body=mapping, doc_type="ndc_collection_item")
    return r


