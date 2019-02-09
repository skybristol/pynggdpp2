#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

# Set variables
sb_catalog_path = "https://www.sciencebase.gov/catalog/items"
sb_vocab_path = "https://www.sciencebase.gov/vocab"
sb_default_max = "100"
sb_default_format = "json"
sb_default_props = "title,body,contacts,spatial,files,webLinks,facets,dates"
ndc_vocab_id = "5bf3f7bce4b00ce5fb627d57"
ndc_catalog_id = "4f4e4760e4b07f02db47dfb4"
queue_name = "q_ndc_collections"


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


def ndc_get_collections(parentId=ndc_catalog_id, fields=sb_default_props, collection_id=None):
    nextLink = f'{sb_catalog_path}?' \
                           f'format={sb_default_format}&' \
                           f'max={sb_default_max}&' \
                           f'fields={fields}&' \
                           f'folderId={parentId}&' \
                           f"filter=tags%3D{ndc_collection_type_tag('ndc_collection',False)}"
    if collection_id is not None:
        nextLink= f"{nextLink}&id={collection_id}"

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


def collection_metadata_summary(collection=None, collection_id=None):
    if collection is None:
        collection_record = ndc_get_collections(collection_id=collection_id, fields="title,contacts,dates")
        if collection_record is None:
            return {"Error": "Collection record could not be retrieved or is not a valid NDC collection"}

        if len(collection_record) > 1:
            return {"Error": "Query for collection returned more than one for some reason"}

        collection_record = collection_record[0]
    else:
        collection_record = collection

    collection_meta = dict()
    collection_meta["ndc_collection_id"] = collection_record["id"]
    collection_meta["ndc_collection_title"] = collection_record["title"]
    collection_meta["ndc_collection_link"] = collection_record["link"]["url"]
    collection_meta["ndc_collection_last_updated"] = next((d["dateString"] for d in collection_record["dates"]
                                                           if d["type"] == "lastUpdated"), None)
    collection_meta["ndc_collection_created"] = next((d["dateString"] for d in collection_record["dates"]
                                                           if d["type"] == "dateCreated"), None)

    if "contacts" in collection_record.keys():
        collection_meta["ndc_collection_owner"] = [c["name"] for c in collection_record["contacts"]
                               if "type" in c.keys() and c["type"] == "Data Owner"]
        if len(collection_meta["ndc_collection_owner"]) == 0:
            collection_meta["ndc_collection_improvements_needed"] = ["Need data owner contact in collection metadata"]
    else:
        collection_meta["ndc_collection_improvements_needed"] = ["Need contacts in collection metadata"]

    return collection_meta