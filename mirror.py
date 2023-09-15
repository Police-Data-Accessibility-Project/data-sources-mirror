#!/usr/bin/env python3

# python imports
from collections import namedtuple
import csv
import datetime
import json
import os
import psycopg2
from psycopg2.extras import execute_values

# third-party imports
from pyairtable import Table

Sheet = namedtuple("Sheet", ['headers', 'rows'])

AGENCIES_TABLE_NAME = "Agencies"
SOURCES_TABLE_NAME = "Data Sources"
COUNTIES_TABLE_NAME = "Counties"

# Functions for writing to Supabase. 
def full_mirror_to_supabase(table_names):
    #Get the data from Airtable, process it, upload to Supabase.
    for table in table_names:
        data = get_full_table_data(table)

        #If data sources table, need to handle separately for link table:
        if table == SOURCES_TABLE_NAME:
            processed, processed_link = process_data_link_full(table, data)
            connect_supabase(processed, table) 
            #This is also where we handle the link table
            connect_supabase(processed_link, "Link Table")

        else:
            processed = process_data_full(table, data)
            connect_supabase(processed, table) 

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
    # "agency_described" -- skipped because we don't need this in supabase
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
        "record_download_option_provided",
        "data_portal_type",
        "access_restrictions",
        "access_restrictions_notes",
        "record_format",
        "update_frequency",
        "update_method",
        "sort_method",
        "agency_described_linked_uid",
        "tags",
        "readme_url",
        "scraper_url",
        "community_data_source",
        "data_source_created",
        "airtable_source_last_modified",
        "url_broken",
        "submission_notes",
        "rejection_note",
        "last_approval_editor",
        "submitter_contact_info",
        "agency_described_submitted",
        "agency_described_not_in_database",
        "approved",
        "record_type_other",
        "data_portal_type_other",
        "private_access_instructions",
        "records_not_online",
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
    elif table_name == "Counties":
        processed = process_counties_full(data.rows)
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

def process_counties_full(data):
    processed = []
    columns = get_full_fieldnames(COUNTIES_TABLE_NAME)
    for source in data:

        row = []
        for field in columns:
            row.append(source.get(field, None))
        processed.append(row)
    return processed


def connect_supabase(processed_data, table_name):
    if table_name == COUNTIES_TABLE_NAME:
        primary_key = "fips"
    elif table_name == "Link Table":
        primary_key = "airtable_uid, agency_described_linked_uid"
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
    #For translating between airtable and supabase table name differences
    supabase_table_names = {COUNTIES_TABLE_NAME: "counties", 
                            AGENCIES_TABLE_NAME: "agencies", 
                            SOURCES_TABLE_NAME: "data_sources",
                            "Link Table": "agency_source_link"}
    
    #Get supabase connection params to create connection
    dbname = os.environ.get("SUPABASE_DBNAME") 
    user = os.environ.get("SUPABASE_USER") 
    host = os.environ.get("SUPABASE_HOST") 
    password = os.environ.get("SUPABASE_PASSWORD") 

    if table_name := supabase_table_names.get(table_name, None):
        print("Updating the", table_name, "table...")
        conn = psycopg2.connect(f"dbname={dbname} user={user} host={host} password={password}")
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

    
#Functions for writing to CSV/JSON (SKIP FOR NOW)
def get_table_data(table_name):
    print(f"getting {table_name} table data ....")
    fieldnames = get_fieldnames(table_name)

    data = Table(
        os.environ["AIRTABLE_KEY"],
        os.environ["AIRTABLE_BASE_ID"],
        table_name
    ).all(fields=fieldnames, formula="{approved}=1")

    # ditch unneeded nesting and get to the objects we care about;
    # nothing should have to care about the original
    # structure beyond this point
    rows = (d["fields"] for d in data)

    return Sheet(fieldnames, rows)

def process_data(table_name, data):
    # this will get a little weird: the Airtable API doesn't include
    # fields with records that have falsy values;
    # each record might have different fields missing than others,
    # and there's no way in advance to determine which ones.
    # so get ready for lots of sniffing and setting of nulls
    # in these processing fxns
    print(f"processing {table_name} data ....")
    if table_name == "Agencies":
        processed = process_agencies(data.rows)
    elif table_name == "Data Sources":
        processed = process_sources(data.rows) 
    else:
        raise RuntimeError("Check the table name -- it might not be accurate")
    return Sheet(data.headers, processed)

def process_agencies(data):
    processed = []
    # doing this here because we only need to do it for agencies and
    # only want to do it after we know there's agencies data
    counties = prep_counties()
    for agency in data:
        # TODO: handle cases of more than one county; we have none at the moment, but it's possible
        encoded_fips = agency.get("county_fips", None)
        decoded_fips = None
        if encoded_fips:
            if type(encoded_fips) == list and len(encoded_fips) > 0:
                encoded_fips_popped = encoded_fips[0]
                if (cfips := counties.get(encoded_fips_popped, None)):
                    decoded_fips = cfips["fips"]        

        county_name = agency.get("county_name", None)
        defunct_year = agency.get("defunct_year", None)
        homepage_url = agency.get("homepage_url", None)
        jurisdiction_type = agency.get("jurisdiction_type", None)
        lat = agency.get("lat", None)
        lng = agency.get("lng", None)
        municipality = agency.get("municipality", None)
        name = agency.get("name", None)
        state_iso = agency.get("state_iso", None)
        submitted_name = agency.get("submitted_name", None)

        row = {
            "name": name,
            "submitted_name": submitted_name,
            "homepage_url": homepage_url,
            "jurisdiction_type": jurisdiction_type,
            "state_iso": state_iso,
            "municipality": municipality,
            "county_fips": decoded_fips,
            "county_name": county_name,
            "lat": lat,
            "lng": lng,
            "defunct_year": defunct_year,
            "airtable_uid": agency["airtable_uid"]
        }

        processed.append(row)
        

    return processed

def process_sources(data):
    processed = []
    for source in data:
        # TODO: handle cases where there is more than one item in the list
        agency_linked = source.get("agency_described_linked_uid", None)
        
        start_raw = source.get("coverage_start", None)
        start = datetime.datetime.strptime(start_raw, "%Y-%m-%d") if start_raw else None

        end_raw = source.get("coverage_end", None)
        end = datetime.datetime.strptime(end_raw, "%Y-%m-%d") if end_raw else None

        access_type = source.get("access_type", None)
        agency_aggregation = source.get("agency_aggregation", None)
        detail_level = source.get("detail_level", None)
        description = source.get("description", None)
        download_option = source.get("record_download_option_provided", None)
        num_records = source.get("number_of_records_available", None)

        originated = None
        if source.get("agency_originated", None):
            if source["agency_originated"] == "yes":
                originated = True
            elif source["agency_originated"] == "no":
                originated = False
        
        # why does this have line endings in the string? c'mon people
        originating_entity = source.get("originating_entity", None)
        portal_type = source.get("data_portal_type", None)
        readme_url = source.get("readme_url", None)
        record_format = source.get("record_format", [])
        record_type = source.get("record_type", None)
        restrictions = source.get("access_restrictions", None)
        restrictions_notes = source.get("access_restrictions_notes", None)

        supplied = None
        if source.get("agency_supplied", None):
            if source["agency_supplied"] == "yes":
                supplied = True 
            elif source["agency_supplied"] == "no":
                supplied = False

        retention_schedule = source.get("retention_schedule", None)
        size = source.get("size", None)
        sort_method = source.get("sort_method", None)

        source_last_updated = None
        if source.get("source_last_updated", None):
            source_last_updated = datetime.datetime.strptime(source["source_last_updated"], "%Y-%m-%d")

        source_url = source.get("source_url", None)
        submitted_name = source.get("submitted_name", None)
        supplying_entity = source.get("supplying_entity", None)
        tags = source.get("tags", [])
        update_frequency = source.get("update_frequency", None)
        update_method = source.get("update_method", None)

        row = {
            "access_restrictions": restrictions,
            "access_restrictions_notes": restrictions_notes,
            "access_type": access_type,
            "agency_described": agency_linked,
            "agency_originated": originated,
            "agency_supplied": supplied,
            "agency_aggregation": agency_aggregation,
            "coverage_start": start,
            "coverage_end": end,
            "data_portal_type": portal_type,
            "description": description,
            "detail_level": detail_level,
            "name": source["name"],
            "number_of_records_available": num_records,
            "originating_entity": originating_entity,
            "readme_url": readme_url,
            "record_download_option_provided": download_option,
            "record_format": record_format,
            "record_type": record_type,
            "retention_schedule": retention_schedule,
            "size": size,
            "sort_method": sort_method,
            "source_last_updated": source_last_updated,
            "source_url": source_url,
            "submitted_name": submitted_name,
            "supplying_entity": supplying_entity,
            "tags": tags,
            "update_frequency": update_frequency,
            "update_method": update_method,
        }

        processed.append(row) 
        
    return processed

def get_fieldnames(name):
    if name == "Agencies":
        return agency_fieldnames()
    elif name == "Data Sources":
        return source_fieldnames()
    else:
        raise RuntimeError("This is not a supported name")

def agency_fieldnames():
    return [
        "name",
        "submitted_name",
        "homepage_url",
        "jurisdiction_type",
        "state_iso",
        "municipality",
        "county_fips",
        "county_name",
        "lat",
        "lng",
        "defunct_year",
        "airtable_uid",
    ]

def source_fieldnames():
    # agency_aggregation -- str
    # detail_level -- str
    return [
        "name",
        "submitted_name",
        "description",
        "record_type",
        "source_url",
        "agency_described",
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
        "record_download_option_provided",
        "data_portal_type",
        "access_restrictions",
        "access_restrictions_notes",
        "record_format",
        "update_frequency",
        "update_method",
        "sort_method",
        "agency_described_linked_uid",
        "tags",
        "readme_url"
    ]

def write_csv(data_package, location):
    with open(location, "w") as f: #may need to add encoding
        writer = csv.DictWriter(f, fieldnames=data_package.headers)
        writer.writeheader()
        writer.writerows(data_package.rows)

def write_json(data_package, location):
    print("writing json ....")
    with open(location, "w+") as f:
        json.dump(data_package, f, indent=4, default=str)

def setup_json(data_dict):
    print("preparing json ....")
    agencies = data_dict[AGENCIES_TABLE_NAME]
    sources = data_dict[SOURCES_TABLE_NAME]
    
    agencies_seen = {}
    for source in sources:
        target_agency = source.get("agency_described", None)
        if target_agency:
            # so fun that they're wrapped in a list of length 1 🙄
            agency_id = target_agency[0]
            if (agency := agencies_seen.get(agency_id, None)):
                source["agency_described"] = agency
            else:
                agency = search_agencies(agency_id, agencies)

                # don't need this anymore and
                # shouldn't get written to the user-facing file
                agency.pop("airtable_uid", None)

                # nest this gently into the source obj
                source["agency_described"] = agency

                # who wants to have to search for it a second time?
                # just make it a lookup
                agencies_seen[agency_id] = agency
        else:
            source["agency_described"] = target_agency
            
    return sources

# a simple scan; ignore anything that doesn't match something we know about
def search_agencies(agency_id, agencies):
    return next(a for a in agencies if a.get("airtable_uid", None) == agency_id)

def mirror_to_csv(table_names, csv_locations, json_location):
    csv_targets = zip(table_names, csv_locations)

    to_json = {}

    for target in csv_targets:
        data = get_table_data(target[0])
        processed = process_data(target[0], data)
        print(f"writing {target} csv")
        write_csv(processed, target[1])

        to_json[target[0]] = processed.rows

    formatted = setup_json(to_json)
    write_json(formatted, json_location)

#-------------------------------------------------------------------------------------------

if __name__ == "__main__":
    create_csv_and_json = True #change this to False if you want to turn this off!
    table_names = [COUNTIES_TABLE_NAME, AGENCIES_TABLE_NAME, SOURCES_TABLE_NAME]
    full_mirror_to_supabase(table_names)

    if create_csv_and_json == True:
        table_names = [AGENCIES_TABLE_NAME, SOURCES_TABLE_NAME]
        agencies_filename = "csv/agencies.csv"
        sources_filename = "csv/data_sources.csv"
        filenames = [agencies_filename, sources_filename]
        json_location = sources_filename.replace("csv", "json")
        mirror_to_csv(table_names, filenames, json_location)
