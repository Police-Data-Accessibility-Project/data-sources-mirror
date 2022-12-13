#!/usr/bin/env python3

# python imports
from collections import namedtuple
import csv
import datetime
import os

# third-party imports
from pyairtable import Table

Sheet = namedtuple("Sheet", ['headers', 'rows'])

def get_table_data(table_name):
    print(f"getting {table_name} table data ....")
    fieldnames = get_fieldnames(table_name)

    # data = Table(
    #     os.environ["AIRTABLE_KEY"],
    #     os.environ["AIRTABLE_BASE_ID"],
    #     table_name
    # ).all(fields=fieldnames, formula="{approved}=1")
    data = Table(
        "keyCtUHdrTsnuvnH0",
        "app473MWXVJVaD7Es",
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
    counties = prep_counties()
    for agency in data:
        encoded_fips = agency.get("county_fips", None)
        if encoded_fips:
            # TODO: handle cases of more than one county
            if type(encoded_fips) == list and len(encoded_fips) > 0:
                encoded_fips_popped = encoded_fips[0]
                if counties.get(encoded_fips_popped, None):
                    decoded_fips = counties.get(encoded_fips_popped, None).get("fips", None)
                else:
                    decoded_fips = None
            else:
                decoded_fips = counties.get(encoded_fips, None).get("fips", None)
        else:
            decoded_fips = None
        county_fips = decoded_fips
        # TODO: handle cases of more than one county
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
            "county_fips": county_fips,
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

        aggregated = None
        if source.get("aggregation_type", None):
            if source["aggregation_type"] == "Individual records" or source["aggregation_type"] == "no":
                aggregated = False
            elif source["aggregation_type"] == "":
                aggregated = None
            else:
                aggregated = True

        description = source.get("description", None)
        download_option = source.get("record_download_option_provided", None)
        num_records = source.get("number_of_records_available", None)

        originated = None
        if source.get("agency_originated", None):
            if source["agency_originated"] == "yes":
                originated = True
            elif source["agency_originated"] == "no":
                originated = False
        
        # why does this have line endings in the string? c/mon people
        originating_entity = source.get("originating_entity", None)
        portal_type = source.get("data_portal_type", None)
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

        supplying_entity = None
        if source.get("supplying_entity", None):
            supplying_entity = [e.strip() for e in source["supplying_entity"]]

        update_frequency = source.get("update_frequency", None)
        update_method = source.get("update_method", None)

        row = {
            "access_restrictions": restrictions,
            "access_restrictions_notes": restrictions_notes,
            "access_type": access_type,
            "agency_described": agency_linked,
            "agency_originated": originated,
            "agency_supplied": supplied,
            "aggregation_type": aggregated,
            "coverage_start": start,
            "coverage_end": end,
            "data_portal_type": portal_type,
            "description": description,
            "name": source["name"],
            "number_of_records_available": num_records,
            "originating_entity": originating_entity,
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
        "airtable_uid"
    ]


def source_fieldnames():
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
        "aggregation_type",
        "coverage_start",
        "coverage_end",
        "source_last_updated",
        "retention_schedule",
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
        "aggregation_type"
    ]


def prep_counties():
    print("making counties ....")
    table_name = "Counties"
    # counties = Table(
    #     os.environ["AIRTABLE_KEY"],
    #     os.environ["AIRTABLE_BASE_ID"],
    #     table_name
    # ).all(fieldnames=["fips", "name", "airtable_uid"])
    counties = Table(
        "keyCtUHdrTsnuvnH0",
        "app473MWXVJVaD7Es",
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


def write_csv(data_package, location):
    with open(location, "w") as f:
        writer = csv.DictWriter(f, fieldnames=data_package.headers)
        writer.writeheader()
        writer.writerows(data_package.rows)


def run_the_jewels(table_names, csv_locations):
    targets = zip(table_names, csv_locations)

    for target in targets:
        data = get_table_data(target[0])
        processed = process_data(target[0], data)
        print(f"writing {target} csv")
        write_csv(processed, target[1])
    

if __name__ == "__main__":
    agencies_filename = "csv/agencies.csv"
    sources_filename = "csv/data_sources.csv"
    filenames = [agencies_filename, sources_filename]

    agencies_table_name = "Agencies"
    sources_table_name = "Data Sources"
    table_names = [agencies_table_name, sources_table_name]

    run_the_jewels(table_names, filenames)
