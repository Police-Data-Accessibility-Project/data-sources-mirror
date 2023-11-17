#!/usr/bin/env python3

# python imports
from collections import namedtuple
import csv
import datetime
import json
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# third-party imports
from pyairtable import Table

Sheet = namedtuple("Sheet", ['headers', 'rows'])

AGENCIES_TABLE_NAME = "Agencies"
SOURCES_TABLE_NAME = "Data Sources"
COUNTIES_TABLE_NAME = "Counties"
REQUESTS_TABLE_NAME = "Data Requests"
VOLUNTEERS_TABLE_NAME = "Volunteers"

# Functions for writing to DigitalOcean. 
def full_mirror_to_digital_ocean(table_names):
    #Get the data from Airtable, process it, upload to DigitalOcean.
    for table in table_names:
        data = get_full_table_data(table)

        #If data sources table, need to handle separately for link table:
        if table == SOURCES_TABLE_NAME:
            processed, processed_link = process_data_link_full(table, data)
            connect_digital_ocean(processed, table) 
            #This is also where we handle the link table
            connect_digital_ocean(processed_link, "Link Table")

        else:
            processed = process_data_full(table, data)
            connect_digital_ocean(processed, table) 

def get_full_table_data(table_name):

    print(f"getting {table_name} table data ....")
    fieldnames = get_full_fieldnames(table_name)

    data = Table(
        os.environ["AIRTABLE_KEY"],
        os.environ["AIRTABLE_BASE_ID"],
        table_name
    ).all()

    # ditch unneeded nesting and get to the objects we care about;
    # nothing should have to care about the original
    # structure beyond this point
    rows = (d["fields"] for d in data)
    return Sheet(fieldnames, rows)

def get_full_fieldnames(name):
    if name == "Agencies":
        return agency_fieldnames_full()
    elif name == "Data Sources":
        return source_fieldnames_full()
    elif name == "Counties":
        return county_fieldnames_full()
    elif name == "Data Requests":
        return requests_fieldnames_full()  
    elif name == "Volunteers":
        return volunteers_fieldnames_full()      
    else:
        raise RuntimeError("This is not a supported name")

def agency_fieldnames_full():
    return [
        "name",
        "homepage_url",
        "count_data_sources",
        "agency_type",
        "multi_agency",
        "submitted_name",
        "jurisdiction_type",
        "state_iso",
        "municipality",
        "zip_code",
        "county_fips",
        "county_name",
        "lat",
        "lng",
        "data_sources",
        "no_web_presence",
        "airtable_agency_last_modified",
        "data_sources_last_updated",
        "approved",
        "rejection_reason",
        "last_approval_editor",
        "submitter_contact",
        "agency_created",
        "county_airtable_uid",
        "defunct_year",
        "airtable_uid",
    ]

def source_fieldnames_full():
    # agency_aggregation -- str
    # detail_level -- str
    # "agency_described" -- skipped because we don't need this in DigitalOcean
    return [
        "name",
        "submitted_name",
        "description",
        "record_type",
        "source_url",
        "airtable_uid", 
        "agency_supplied",
        "supplying_entity",
        "agency_originated",
        "originating_entity",
        "agency_aggregation",
        "coverage_start",
        "coverage_end",
        "source_last_updated",
        "retention_schedule",
        "detail_level",
        "number_of_records_available",
        "size",
        "access_type",
        "data_portal_type",
        "access_notes",
        "record_format",
        "update_frequency",
        "update_method",
        "agency_described_linked_uid",
        "tags",
        "readme_url",
        "scraper_url",
        "data_source_created",
        "airtable_source_last_modified",
        "submission_notes",
        "rejection_note",
        "last_approval_editor",
        "submitter_contact_info",
        "agency_described_submitted",
        "agency_described_not_in_database",
        "approval_status",
        "record_type_other",
        "data_portal_type_other",
        "data_source_request",
        "url_button",
        "tags_other"
    ]

def county_fieldnames_full():
    return [
        "fips",
        "name",
        "name_ascii",
        "state_iso",
        "lat",
        "lng",
        "population",
        "agencies",
        "airtable_uid",
        "airtable_county_last_modified",
        "airtable_county_created"
    ]

def requests_fieldnames_full():
    return [
        "request_id",
        "submission_notes",
        "request_status",
        "submitter_contact_info",
        "agency_described_submitted",
        "record_type",
        "archive_reason",
        "date_created",
        "status_last_changed"
    ]

def volunteers_fieldnames_full():
    return [
        "email",
        "discord",
        "name",
        "help_topics",
        "status",
        "geographic_interest",
        "submission_notes",
        "internal_notes",
        "last_contacted",
        "github",
        "created_by",
        "created"
    ]

def process_data_link_full(table_name, data):
    print(f"processing {table_name} data ....")
    processed, processed_link = process_sources_full(data.rows) 
    return Sheet(data.headers, processed), Sheet(["airtable_uid", "agency_described_linked_uid"], processed_link)

def process_sources_full(data):
    processed = []
    processed_link = [] #for the link table

    columns = get_full_fieldnames(SOURCES_TABLE_NAME)
    for source in data:

        row = []
        for field in columns:
            #For the link table:
            if field == "agency_described_linked_uid":
                agency_linked = source.get(field, None)
            elif field == "airtable_uid":
                airtable_uid = source.get(field, None)
                row.append(airtable_uid)
            else:
                row.append(source.get(field, None))

        #if there is a linked agency, save it to the link table
        if agency_linked:
            #Sometimes there are multiple linked agencies, we want to capture each one
            for i in range(len(agency_linked)):
                link_row = [airtable_uid, agency_linked[i]]
                processed_link.append(link_row)

        processed.append(row) 
    return processed, processed_link

def process_data_full(table_name, data):

    print(f"processing {table_name} data ....")
    if table_name == "Agencies":
        processed = process_agencies_full(data.rows)
    elif table_name in ("Counties", "Data Requests", "Volunteers"):
        processed = process_standard_full(table_name, data.rows)   
    else:
        raise RuntimeError("Check the table name -- it might not be accurate")
    return Sheet(data.headers, processed)

def process_agencies_full(data):
    processed = []
   
    # doing this here because we only need to do it for agencies and
    # only want to do it after we know there's agencies data
    # (get counties fips codes from their airtable uids)
    counties = prep_counties()
        
    for agency in data:
        
        columns = get_full_fieldnames(AGENCIES_TABLE_NAME)
        row = []
        for field in columns:
            # TODO: handle cases of more than one county; we have none at the moment, but it's possible  
            if field == "county_fips":
                row.append(process_county(field, agency, counties))
            elif field == "county_airtable_uid":
                row.append(process_county_uid(field, agency))
            else:
                row.append(agency.get(field, None))

        processed.append(row)
        

    return processed

#For getting county fips in agencies table
def prep_counties():
    table_name = "Counties"
    counties = Table(
        os.environ["AIRTABLE_KEY"],
        os.environ["AIRTABLE_BASE_ID"],
        table_name
    ).all(fields=["fips", "name", "airtable_uid"])

    # might be more we can do here to be useful
    cleaned = (
        c["fields"]
        for c in counties
    )

    return {
        county["airtable_uid"]: {
            "fips": county["fips"],
            "name": county["name"]
        }
        for county in cleaned
    }

#handling specific cases
def process_county(column, agency, counties):
    encoded_fips = agency.get(column, None)
    decoded_fips = None
    if encoded_fips:
        if type(encoded_fips) == list and len(encoded_fips) > 0:
            encoded_fips_popped = encoded_fips[0]
            if (cfips := counties.get(encoded_fips_popped, None)):
                decoded_fips = cfips["fips"]
    return decoded_fips

def process_county_uid(column, agency):
    #get the string rep, it's in a list of one
    if county_airtable_uid := agency.get(column, None):
        return county_airtable_uid[0]

def process_standard_full(table_name, data):
    processed = []
    columns = get_full_fieldnames(table_name)
    for source in data:

        row = []
        for field in columns:
            row.append(source.get(field, None))
        processed.append(row)
    return processed


def connect_digital_ocean(processed_data, table_name):
    if table_name == COUNTIES_TABLE_NAME:
        primary_key = "fips"
    elif table_name == "Link Table":
        primary_key = "airtable_uid, agency_described_linked_uid"
    elif table_name == REQUESTS_TABLE_NAME:
        primary_key = "request_id"
    elif table_name == VOLUNTEERS_TABLE_NAME:
        primary_key = "name"
    else:
        primary_key = "airtable_uid"
    if table_name == "Link Table":
        headers = ["airtable_uid", "agency_described_linked_uid"]
    else:
        headers = [h for h in processed_data[0] if h != "agency_described_linked_uid"]
    headers_no_id = [h for h in headers if h != primary_key]
    if table_name == "Link Table":
        conflict_clause = "nothing"
    else:
        set_headers = ", ".join(headers_no_id)
        excluded_headers = "(EXCLUDED." + ", EXCLUDED.".join(headers_no_id) + ")"
        conflict_clause = f"update set ({set_headers}) = {excluded_headers}"
    processed_records = processed_data[1]
    #For translating between airtable and DigitalOcean table name differences
    digital_ocean_table_names = {COUNTIES_TABLE_NAME: "counties", 
                            AGENCIES_TABLE_NAME: "agencies", 
                            SOURCES_TABLE_NAME: "data_sources",
                            REQUESTS_TABLE_NAME: "data_requests",
                            VOLUNTEERS_TABLE_NAME: "volunteers",
                            "Link Table": "agency_source_link"}
    
    #Get DigitalOcean connection params to create connection
    DIGITAL_OCEAN_DATABASE_URL = os.getenv('DO_DATABASE_URL')

    if table_name := digital_ocean_table_names.get(table_name, None):
        print("Updating the", table_name, "table...")
        conn = psycopg2.connect(DIGITAL_OCEAN_DATABASE_URL)
        with conn.cursor() as curs:
            for record in processed_records:
                clean_record = []
                for field in record:
                    if type(field) in (list, dict):
                        clean_field = json.dumps(field).replace("'","''")
                        clean_record.append(f"'{clean_field}'")
                    elif field is None:
                        clean_record.append("NULL")
                    elif type(field) == str:
                        clean_field = field.replace("'","''")
                        clean_record.append(f"'{clean_field}'")
                    else:
                        clean_record.append(f"'{field}'")
                query = f"insert into {table_name} ({', '.join(headers)}) values ({', '.join(clean_record)}) on conflict ({primary_key}) do {conflict_clause}"
                curs.execute(query)
        conn.commit()
        conn.close()
    else:
        print("Unexpected table name!")


if __name__ == "__main__":
    table_names = [COUNTIES_TABLE_NAME, AGENCIES_TABLE_NAME, SOURCES_TABLE_NAME, REQUESTS_TABLE_NAME, VOLUNTEERS_TABLE_NAME]
    full_mirror_to_digital_ocean(table_names)


