#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pynggdpp import collection as ndcCollection

import argparse
import sys
import logging
import requests
from geojson import Feature, Point, Polygon, FeatureCollection
from geojson import dumps as geojson_dumps
from geojson_utils import centroid
from bs4 import BeautifulSoup
import xmltodict
from datetime import datetime
import dateutil.parser as dt_parser
from gis_metadata.metadata_parser import get_metadata_parser
import json
import csv
import pandas as pd
import validators


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


def parse_waf(link_meta):
    """
    Parses a Web Accessible Folder HTML response to build a data structure containing response header information and
    a listing of potentially harvestable XML file URLs. This sets up a list of harvestable items for a given collection,
    including the available date/time information that can be used to keep things in sync over time.

    :param link_meta: Web link object containing a URL and other properties from the ScienceBase Item schema
    :return: Dictionary containing headers and url_list
    """
    try:
        if not isinstance(link_meta, dict):
            raise Exception("Link metadata needs to be a dictionary object")

        if not validators.url(link_meta["uri"]):
            raise Exception(f"URL is not valid - {link_meta['uri']}")

        r = requests.get(link_meta['uri'])

        waf_package = dict()
        waf_package["link_meta"] = link_meta
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
                        item["file_url"] = f"{link_meta['uri']}{v}"
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
                            item["file_url"] = f"{link_meta['uri']}{cell_text}"
                        elif i == 2:
                            item["file_date"] = dt_parser.parse(cell_text).isoformat()
                        elif i == 3:
                            item["file_size"] = cell_text
                    if "file_name" in item.keys() and item["file_name"].split(".")[-1] == "xml":
                        waf_package["url_list"].append(item)

        return waf_package

    except Exception as e:
        return e


def inspect_response(response):
    """
    Build packet of information from HTTP response to add to file or link metadata

    :param response: HTTP response object from requests
    :return: Dictionary containing packaged response properties
    """

    response_meta = dict()

    # Add header information
    for k, v in response.headers.items():
        response_meta[k] = v

    # Create basic classification of file type for convenience
    response_meta["file-class"] = "Unknown"
    if "Content-Type" in response_meta.keys():
        if response_meta["Content-Type"].find("/") != -1:
            response_meta["file-class"] = response_meta["Content-Type"].split("/")[0]

    return response_meta


def inspect_response_content(response, response_meta=None):
    """
    Runs a file introspection process to evaluate file contents and build a set of properties for further evaluation.

    :param response: HTTP response object from requests
    :param response_meta: Dictionary containing the basic response metadata; required for this function to run but
    assembled here if not provided
    :return: Dictionary containing results of file introspection
    """

    introspection_meta = dict()

    # Grab the basic response metadata if not provided
    if response_meta is None:
        response_meta = inspect_response(response)

    # Pull out the first line of text file responses and analyze for content
    if response_meta["file-class"] == "text":
        if response.encoding is None:
            response.encoding = 'latin1'

        for index,line in enumerate(response.iter_lines(decode_unicode=True)):
            if index > 0:
                break
            if line:
                introspection_meta["first_line"] = line

        introspection_meta["encoding"] = response.encoding

        # Check for quote characters
        if introspection_meta["first_line"].find('"') != -1:
            introspection_meta["quotechar"] = '"'
            introspection_meta["first_line"] = introspection_meta["first_line"].replace('"', '')

        # Set delimiter character and generate list of fields
        if introspection_meta["first_line"].find("|") != -1:
            introspection_meta["field_names"] = introspection_meta["first_line"].split("|")
            if len(introspection_meta["field_names"]) > 4:
                introspection_meta["delimiter"] = "|"
        elif introspection_meta["first_line"].find(",") != -1:
            introspection_meta["field_names"] = introspection_meta["first_line"].split(",")
            if len(introspection_meta["field_names"]) > 4:
                introspection_meta["delimiter"] = ","

        # Handle cases where there is something weird in the file with field names
        if "field_names" in introspection_meta.keys():
            if len(introspection_meta["field_names"]) > 30 or len(introspection_meta["first_line"]) > 1000:
                introspection_meta["First Line Extraction Error"] = \
                    f"Line Length - {len(introspection_meta['first_line'])}, " \
                    f"Number Fields - {len(introspection_meta['field_names'])}"
                del introspection_meta["field_names"]
                del introspection_meta["first_line"]
                del introspection_meta["delimiter"]

    elif response_meta["file-class"] == "application":
        if response_meta["Content-Type"] == "application/xml":
            try:
                dictData = xmltodict.parse(response.text, dict_constructor=dict)
                top_keys = list(dictData.keys())
                if len(top_keys) == 1:
                    second_keys = list(dictData[top_keys[0]].keys())
                    if isinstance(dictData[top_keys[0]][second_keys[-1]], list):
                        introspection_meta['record_container_path'] = [top_keys[0], second_keys[-1]]
                        introspection_meta['record_number'] = len(dictData[top_keys[0]][second_keys[-1]])
                        introspection_meta['field_names'] = list(dictData[top_keys[0]][second_keys[-1]][0].keys())
                if "record_container_path" not in introspection_meta.keys():
                    introspection_meta["message"] = "Could not establish processable record container in XML file"
            except Exception as e:
                introspection_meta["message"] = f"Could not parse XML file with xmltodict - {e}"


        else:
            introspection_meta["message"] = "Application types other than XML not yet implemented"

    else:
        introspection_meta["message"] = "Response contained nothing to process"

    return introspection_meta


def check_processable(explicit_source_files, source_meta, content_meta):
    """
    Evaluates source and processed metadata for file content and determines whether the content is processable.

    :param explicit_source_files: List of explicitly flagged source files (pathOnDisk) evaluated for a list of
    ScienceBase Files
    :param source_meta: Source metadata structure (ScienceBase File object)
    :param response_meta: Metadata from the HTTP response for the file object from inspect_response()
    :param content_meta: Metadata from the file content introspection from inspect_response_content()
    :return: Dictionary containing process parameters
    """
    process_meta = dict()
    process_meta["date_checked"] = datetime.utcnow().isoformat()

    # Pre-flag whether or not we think this is a processable route to collection records
    if len(explicit_source_files) > 0:
        if source_meta["pathOnDisk"] in explicit_source_files:
            process_meta["Processable Route"] = True
        else:
            process_meta["Processable Route"] = False
    else:
        process_meta["Processable Route"] = True

        # Set processable route to false if we were not able to extract field names from either CSV or XML
        if "field_names" not in content_meta.keys():
            process_meta["Processable Route"] = False

    return process_meta


def file_meta(collection_id, files):
    """
     Retrieve and package metadata for a set of files (expects list of file objects in ScienceBase Item format).

    :param collection_id: (str) ScienceBase ID of the collection.
    :param files: (list of dicts) file objects in a list from the ScienceBase collection item
    :return: (list of dicts) file metadata with value-added processing information

    In the workflow, this function is used to pre-build ndc_files, a database collection
    where file information is cached for later processing. It processes through the file objects in the list and
    adds information based on retrieving and examining the file via its url.
    """

    # Put a single file object into a list
    if isinstance(files, dict):
        files = [files]

    # Check for explicit Source Data flagging in the collection
    explicit_source_files = [f["pathOnDisk"] for f in files if "title" in f.keys() and f["title"] == "Source Data"]

    # Set up evaluated file list
    evaluated_file_list = list()

    for file in files:
        metadata = dict()

        # Copy in all source properties
        metadata["source_meta"] = file

        # Add context
        metadata["collection_id"] = collection_id

        # Retrieve the file to check things out
        response = requests.get(file["url"], stream=True)

        # Inspect the response and return metadata
        metadata["response_meta"] = inspect_response(response)

        # Inspect the response content and return metadata
        metadata["content_meta"] = inspect_response_content(response, response_meta=metadata["response_meta"])

        # Determine whether or not the file route is a processable one for the NDC
        metadata["process_meta"] = check_processable(explicit_source_files,
                                                     metadata["source_meta"],
                                                     metadata["content_meta"])

        evaluated_file_list.append(metadata)

    return evaluated_file_list


def link_meta(collection_id, webLinks=None):
    """
    Processes the web links of a collection to determine their potential as a route for collection items and set
    things up for processing.

    :param collection_id: (str) ScienceBase Item ID of the collection
    :param webLinks: (list of dicts) List of webLink objects in the ScienceBase format; if not supplied, the function
    will execute a function to retrieve the webLinks for a collection
    :return: In the workflow, this function is used to pre-build the ndc_weblinks, a database collection where link
    information is cached for later processing. It processes through the webLink objects in the list and adds
    information based on retrieving and examining the webLink via its uri.
    """

    # Set up an error container. If the function return is a single dict instead of a list of dicts, that signals
    # a problem
    error_container = dict()
    error_container["collection_id"] = collection_id

    # Put a single webLink object into a list
    if isinstance(webLinks, dict):
        webLinks = [webLinks]

    # Get files if not provided
    if webLinks is None:
        collections = ndcCollection.ndc_get_collections(collection_id=collection_id, fields="title,contacts,webLinks")
        if collections is None or len(collections) == 0:
            error_container["error"] = "Cannot run without files being provided"
            return error_container

        if "webLinks" not in collections[0].keys():
            error_container["error"] = "No webLinks found in the collection to evaluate"
            return error_container

        collection = collection[0]
        webLinks = collection["webLinks"]

    # Set up evaluated webLink list
    evaluated_link_list = list()

    for link in webLinks:
        metadata = dict()

        # Copy in all source properties
        metadata["source_meta"] = link

        # Add context
        metadata["collection_id"] = collection_id
        metadata["Date Checked"] = datetime.utcnow().isoformat()

        # Set up the response container
        metadata["response_meta"] = dict()

        # We go ahead and test every link to at least get a current status code
        try:
            response = requests.get(link["uri"])
            metadata["response_meta"]["status_code"] = response.status_code
        except Exception as e:
            metadata["response_meta"]["error"] = str(e)

        # For now, we're just teasing out and doing something special with WAF links
        # These are explicitly identified for the few cases as WAF type link
        if "response" in locals() and response.status_code == 200:
            if "type" in link.keys() and link["type"] == "WAF":
                metadata["response_meta"]["apparent_encoding"] = response.apparent_encoding
                for k,v in response.headers.items():
                    metadata["response_meta"][k] = v
                metadata["content_meta"] = dict()
                metadata["content_meta"]["waf_links"] = list()
                for url in list_waf(url=link["uri"], response=response):
                    metadata["content_meta"]["waf_links"].append({"url": url})

        evaluated_link_list.append(metadata)

    return evaluated_link_list


def nggdpp_xml_to_dicts(file_meta):
    """
    Retrieve a processable NGGDPP XML file and convert it's contents to a list of dictionaries

    :param file_meta: (dict) File metadata structure created with the file_meta() function
    :return: The expected output of this function is a list of dicts (each record in the dataset). A single dict
    returned indicates that an error occurred, and that is supplied in the return.
    """

    try:
        if file_meta["response_meta"]["Content-Type"] != "application/xml":
            raise Exception("File content type is not application/xml")

        if not isinstance(file_meta['content_meta']['record_container_path'], list):
            raise Exception("No record container path recorded for file")

        if not len(file_meta['content_meta']['record_container_path']) == 2:
            raise Exception("Record container path does not contain the proper number of items")

        xml_data = requests.get(file_meta["source_meta"]["url"]).text
        d_data = xmltodict.parse(xml_data, dict_constructor=dict)

        xml_tree_top = file_meta['content_meta']['record_container_path'][0]
        xml_tree_next = file_meta['content_meta']['record_container_path'][1]
        record_list = d_data[xml_tree_top][xml_tree_next]

        if not isinstance(record_list, list):
            raise Exception("Could not establish list of records in XML data")

        if len(record_list) == 0:
            raise Exception("File processing produced an empty list of records")

        if not isinstance(record_list[0], dict):
            raise Exception("First element of record list is not a dictionary")

        return record_list

    except Exception as e:
        return e


def nggdpp_text_to_dicts(file_meta):
    """
    Retrieve a processable NGGDPP text file and convert it's contents to a list of dictionaries

    :param file_meta: (dict) File metadata structure created with the file_meta() function
    :return: The expected output of this function is a list of dicts (each record in the dataset). A single dict
    returned indicates that an error occurred, and an error message is included in the return.
    """

    if file_meta["response_meta"]["file-class"] != "text":
        output = {"Error": "Cannot process non-text files at this time"}
    else:
        if "delimiter" not in file_meta["content_meta"].keys():
            output = {"Error": "Cannot process text files with an indeterminate delimiter"}
        else:
            try:
                df_file = pd.read_csv(file_meta["source_meta"]["url"],
                                      sep=file_meta["content_meta"]["delimiter"],
                                      encoding=file_meta["content_meta"]["encoding"],
                                      error_bad_lines=False,
                                      quoting=csv.QUOTE_NONE)
            except UnicodeDecodeError:
                # Catches cases where there are characters with different encoding within some types of Windows files
                # latin1 deals with everything but may introduce weird characters into the final output
                df_file = pd.read_csv(file_meta["source_meta"]["url"],
                                      sep=file_meta["content_meta"]["delimiter"],
                                      encoding="latin1",
                                      error_bad_lines=False,
                                      quoting=csv.QUOTE_NONE)
            finally:
                output = {"Error": "Cannot read and process text file with current encoding"}

        if "df_file" in locals():
            # Drop all the rows with empty cells.
            df_file = df_file.dropna(how="all")

            # Drop all unnamed columns
            df_file.drop(df_file.columns[df_file.columns.str.contains('unnamed', case=False)], axis=1)

            # Outputting the dataframe to JSON and then loading to a list of dicts makes for the cleanest eventual
            # GeoJSON.
            json_file = df_file.to_json(orient="records")

            # Set source list to a list of dictionaries from the JSON construct
            output = json.loads(json_file)

        return output


def nggdpp_record_list_to_geojson(record_source, file_meta):
    """
    Take a list of dictionaries containing NGGDPP records, convert to GeoJSON features, and return a Feature Collection

    :param record_source: List of dictionaries from either file processing or WAF processing
    :param file_meta: File metadata dictionary from the ndc_files pre-processing
    :return: Note: The expected output of this function is a dictionary containing the original file_meta structure,
    a GeoJSON feature collection, and a processing log containing a report of the process. In the workflow, the
    processing log is added to the collection record in the ndc_log data store, and the features from the
    feature collection are added to their own collection (using the collection ID as name) and piped into
    ElasticSearch for use.
    """

    # Set up a processing report to record what happens
    processing_meta = dict()
    processing_meta["collection_id"] = file_meta["collection_id"]
    processing_meta["source_file_url"] = file_meta["source_meta"]["url"]
    processing_meta["Number of Errors"] = 0

    feature_list = []
    for p in record_source:
        # Lower-casing the keys from the original data makes things simpler
        p = {k.lower(): v for k, v in p.items()}

        # Infuse file properties
        p["ndc_source_file"] = file_meta["source_meta"]["url"]
        p["ndc_source_file_date"] = file_meta["source_meta"]["dateUploaded"]

        # Add date this record was produced from source
        p["ndc_record_build_date"] = datetime.utcnow().isoformat()

        # Infuse collection-level metadata properties
        p["ndc_collection_id"] = file_meta["collection_id"]
        for k, v in file_meta["collection_meta"].items():
            p[k] = v

        # Set up a processing errors container
        p["ndc_processing_errors"] = []

        # For now, I opted to take alternate forms of coordinates and put them into a coordinates property to keep
        # with the same overall processing logic
        if "coordinates" not in p.keys():
            if ("latitude" in p.keys() and "longitude" in p.keys()):
                p["coordinates"] = f'{p["longitude"]},{p["latitude"]}'

        # Set default empty geometry
        g = Point(None)

        # Try to make point geometry from the coordinates
        # This is where I'm still running into some errors I need to go back and work through
        if "coordinates" in p.keys():
            try:
                if "," in p["coordinates"]:
                    g = build_point_geometry(p["coordinates"])
            except Exception as e:
                this_error = processing_error("geometry",
                                                    f"{e}; {str(p['coordinates'])}; kept empty geometry")
                p["ndc_processing_errors"].append(this_error)
                processing_meta["Number of Errors"] += 1

        # Add feature to list
        feature_list.append(build_ndc_feature(g, p))

    # Add some extra file processing metadata
    processing_meta["date_processed"] = datetime.utcnow().isoformat()
    processing_meta["record_number"] = len(feature_list)

    # Make a Feature Collection from the feature list
    # We don't really need to do this since it's not how we are storing the data, but it makes this function usable
    # beyond our immediate use case.
    processing_package = dict()
    processing_package["processing_meta"] = processing_meta
    processing_package["feature_collection"] = json.loads(geojson_dumps(FeatureCollection(feature_list)["features"]))

    return processing_package


def processing_error(property, error_str):
    """
    Build a simple error structure indicating the section of the data model and the error that occurred.

    Args:
        property: (str) Property in the data model where the error was found
        error_str: (str) Error string to record

    Note: This is used in the data processing flow to indicate record level issues with a given property that were
    not fatal but were recorded for later action.
    """

    error = dict()
    error[property] = error_str
    error['DateTime'] = datetime.utcnow().isoformat()

    return error


def process_log_entry(collection_id, processing_meta, errors=None):
    """
    Build a process log entry to drop in the process_log data store. Information logged here is for each time
    a source file/waf/etc is processed for a given collection. It records some basic information about what happened
    in the process.

    :param collection_id: (str) Collection identifier
    :param source_meta: (dict) processing metadata packet
    :param errors: (list of dicts) Any errors that came up in processing
    :return: Returns a log entry packaged up and ready to record
    """

    log_entry = dict()

    log_entry["collection_id"] = collection_id
    log_entry["date_stamp"] = datetime.utcnow().isoformat()
    log_entry["processing_meta"] = processing_meta

    if errors is not None:
        log_entry["errors"] = errors

    return log_entry


def feature_from_metadata(collection_meta, link_meta):
    """
    Parses FGDC-CSDGM, ISO19115, and ArcGIS metadata XML responses retrieved via HTTP from a WAF and builds a GeoJSON
    feature with properties in common across the 3 metadata dialects. Because of our use case in working with point
    geometry at this point, the function attempts to build a point from a supplied bounding box in the metadata. It
    deals with a convention used in the National Geothermal Data System of building a "minimal" bounding box with the
    same east/west and south/north coordinates, using one "corner" for the point. In the case of an actual bounding
    box, the function generates a centroid and uses that as the feature geometry.

    :param collection_meta: Dictionary containing a summary of the collection metadata
    :param link_meta: Dictionary containing properties about the link generated with parse_waf()
    :return: GeoJSON point feature built from harvested FGDC or ISO XML metadata document

    Note: This function will need quite a bit of refinement once we start working with the feature collections and
    determine what all properties we want to work with. We may need to dig deeper than the few high-level abstract
    properties the gis_metadata parser tools pull out.
    """

    try:
        if not validators.url(link_meta["file_url"]):
            raise Exception(f"URL is not valid - {link_meta['file_url']}")

        if not isinstance(collection_meta, dict):
            raise Exception("Collection metadata dictionary must be provided")

        if not isinstance(link_meta, dict):
            raise Exception("Link metadata dictionary must be provided")

        meta_response = requests.get(link_meta["file_url"])

        # Declare a dictionary structure to contain the properties
        p = dict()

        # Add NDC-specific object to contain infused properties
        p["ndc_meta"] = dict()

        # Infuse collection metadata properties
        p["ndc_meta"].update(collection_meta)

        # Infuse link metadata properties
        p["ndc_meta"].update(link_meta)

        # Add processing date for reference
        p["ndc_meta"]["date_created"] = datetime.utcnow().isoformat()

        # Add in the Last-Modified date from the HTTP response if available, which will be useful in checking for updates
        # Last-Modified is not always provided in the HTTP response depending on the sending server
        if "Last-Modified" in meta_response.headers.keys():
            p["ndc_meta"]["source_file_last_modified"] = \
                dt_parser.parse(meta_response.headers["Last-Modified"]).isoformat()

        # Parse the metadata using the gis_metadata tools
        parsed_metadata = get_metadata_parser(meta_response.text)

        # Add any and all properties that aren't blank (ref. gis_metadata.utils.get_supported_props())
        if len(parsed_metadata.abstract) > 0:
            p['abstract'] = parsed_metadata.abstract
        if len(parsed_metadata.resource_desc) > 0:
            p['resource_desc'] = parsed_metadata.resource_desc
        if len(parsed_metadata.place_keywords) > 0:
            p['place_keywords'] = parsed_metadata.place_keywords
        if len(parsed_metadata.contacts) > 0:
            p['contacts'] = parsed_metadata.contacts
        if len(parsed_metadata.use_constraints) > 0:
            p['use_constraints'] = parsed_metadata.use_constraints
        if len(parsed_metadata.originators) > 0:
            p['originators'] = parsed_metadata.originators
        if len(parsed_metadata.dist_contact_person) > 0:
            p['dist_contact_person'] = parsed_metadata.dist_contact_person
        if len(parsed_metadata.dist_liability) > 0:
            p['dist_liability'] = parsed_metadata.dist_liability
        if len(parsed_metadata.dist_contact_org) > 0:
            p['dist_contact_org'] = parsed_metadata.dist_contact_org
        if len(parsed_metadata.dist_email) > 0:
            p['dist_email'] = parsed_metadata.dist_email
        if len(parsed_metadata.dist_address) > 0:
            p['dist_address'] = parsed_metadata.dist_address
        if len(parsed_metadata.dist_phone) > 0:
            p['dist_phone'] = parsed_metadata.dist_phone
        if len(parsed_metadata.purpose) > 0:
            p['purpose'] = parsed_metadata.purpose
        if len(parsed_metadata.bounding_box) > 0:
            p['bounding_box'] = parsed_metadata.bounding_box
        if len(parsed_metadata.stratum_keywords) > 0:
            p['stratum_keywords'] = parsed_metadata.stratum_keywords
        if len(parsed_metadata.temporal_keywords) > 0:
            p['temporal_keywords'] = parsed_metadata.temporal_keywords
        if len(parsed_metadata.attributes) > 0:
            p['attributes'] = parsed_metadata.attributes
        if len(parsed_metadata.online_linkages) > 0:
            p['online_linkages'] = parsed_metadata.online_linkages
        if len(parsed_metadata.processing_instrs) > 0:
            p['processing_instrs'] = parsed_metadata.processing_instrs
        if len(parsed_metadata.dates) > 0:
            p['dates'] = parsed_metadata.dates
        if len(parsed_metadata.tech_prerequisites) > 0:
            p['tech_prerequisites'] = parsed_metadata.tech_prerequisites
        if len(parsed_metadata.process_steps) > 0:
            p['process_steps'] = parsed_metadata.process_steps
        if len(parsed_metadata.processing_fees) > 0:
            p['processing_fees'] = parsed_metadata.processing_fees
        if len(parsed_metadata.title) > 0:
            p['title'] = parsed_metadata.title
        if len(parsed_metadata.thematic_keywords) > 0:
            p['thematic_keywords'] = parsed_metadata.thematic_keywords
        if len(parsed_metadata.dist_state) > 0:
            p['dist_state'] = parsed_metadata.dist_state
        if len(parsed_metadata.data_credits) > 0:
            p['data_credits'] = parsed_metadata.data_credits
        if len(parsed_metadata.dist_country) > 0:
            p['dist_country'] = parsed_metadata.dist_country
        if len(parsed_metadata.dataset_completeness) > 0:
            p['dataset_completeness'] = parsed_metadata.dataset_completeness
        if len(parsed_metadata.raster_info) > 0:
            p['raster_info'] = parsed_metadata.raster_info
        if len(parsed_metadata.publish_date) > 0:
            p['publish_date'] = parsed_metadata.publish_date
        if len(parsed_metadata.dist_city) > 0:
            p['dist_city'] = parsed_metadata.dist_city
        if len(parsed_metadata.dist_postal) > 0:
            p['dist_postal'] = parsed_metadata.dist_postal
        if len(parsed_metadata.attribute_accuracy) > 0:
            p['attribute_accuracy'] = parsed_metadata.attribute_accuracy
        if len(parsed_metadata.larger_works) > 0:
            p['larger_works'] = parsed_metadata.larger_works
        if len(parsed_metadata.supplementary_info) > 0:
            p['supplementary_info'] = parsed_metadata.supplementary_info

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

    except Exception as e:
        return e


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="Just a Fibonnaci demonstration")
    parser.add_argument(
        '--version',
        action='version',
        version='pynggdpp {ver}'.format(ver=__version__))
    parser.add_argument(
        dest="n",
        help="n-th Fibonacci number",
        type=int,
        metavar="INT")
    parser.add_argument(
        '-v',
        '--verbose',
        dest="loglevel",
        help="set loglevel to INFO",
        action='store_const',
        const=logging.INFO)
    parser.add_argument(
        '-vv',
        '--very-verbose',
        dest="loglevel",
        help="set loglevel to DEBUG",
        action='store_const',
        const=logging.DEBUG)
    return parser.parse_args(args)


def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """
    args = parse_args(args)
    setup_logging(args.loglevel)
    _logger.debug("Starting crazy calculations...")
    print("The {}-th Fibonacci number is {}".format(args.n, fib(args.n)))
    _logger.info("Script ends here")


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
