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

    data = Table(
        os.environ["AIRTABLE_KEY"],
        os.environ["AIRTABLE_BASE_ID"],
        table_name
    ).all(fields=fieldnames, formula=os.environ["FORMULA"])

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
        print("processing sources ....")
        processed = process_sources(data.rows)
    else:
        raise RuntimeError("Check the table name -- it might not be accurate")

    return Sheet(data.headers, processed)


def process_agencies(data):
    processed = []
    for agency in data:
        county_fips = None
        if agency.get("county_fips", None):
            county_fips = agency["county_fips"]

        county_name = None
        if agency.get("county_name", None):
            county_name = agency["county_name"]

        defunct_year = None
        if agency.get("defunct_year", None):
            defunct_year = agency["defunct_year"]
        
        homepage_url = None
        if agency.get("homepage_url", None):
            homepage_url = agency["homepage_url"]

        jurisdiction_type = None
        if agency.get("jurisdiction_type", None):
            jurisdiction_type = agency["jurisdiction_type"]

        lat = None
        if agency.get("lat", None):
            lat = agency["lat"]

        lng = None
        if agency.get("lng", None):
            lng = agency["lng"]

        municipality = None
        if agency.get("municipality", None):
            municipality = agency["municipality"]

        name = None
        if agency.get("name", None):
            name = agency["name"]

        state_iso = None
        if agency.get("state_iso", None):
            state_iso = agency["state_iso"]

        submitted_name = None
        if agency.get("submitted_name", None):
            submitted_name = agency["submitted_name"]

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
        agency_linked = None
        if source.get("agency_described_linked_uid", None):
            # TODO: handle cases where there is more than one item in the list
            agency_linked = source["agency_described_linked_uid"]

        start_raw = source.get("coverage_start", None)
        start = datetime.datetime.strptime(start_raw, "%Y-%m-%d") if start_raw else None

        end_raw = source.get("coverage_end", None)
        end = datetime.datetime.strptime(end_raw, "%Y-%m-%d") if end_raw else None

        access_type = None
        if source.get("access_type", None):
            access_type = source["access_type"]

        aggregated = None
        if source.get("aggregation_type", None):
            if source["aggregation_type"] == "Individual records" or source["aggregation_type"] == "no":
                aggregated = False
            elif source["aggregation_type"] == "":
                aggregated = None
            else:
                aggregated = True

        description = None
        if source.get("description", None):
            description = source["description"]

        download_option = None
        if source.get("record_download_option_provided", None):
            download_option = source["record_download_option_provided"]

        num_records = None
        if source.get("number_of_records_available", None):
            num_records = int(source["number_of_records_available"])

        originated = None
        if source.get("agency_originated", None):
            if source["agency_originated"] == "yes":
                originated = True
            elif source["agency_originated"] == "no":
                originated = False
        
        originating_entity = None
        if source.get("originating_entity", None):
            # why does this have line endings in the string? c/mon people
            originating_entity = source["originating_entity"].strip()

        portal_type = None
        if source.get("data_portal_type", None):
            portal_type = source["data_portal_type"]

        record_format = []
        if source.get("record_format", None):
            record_format = source["record_format"]

        record_type = None
        if source.get("record_type", None):
            record_type = source["record_type"]

        restrictions = None
        if source.get("access_restrictions", None):
            restrictions = source["access_restrictions"]

        restrictions_notes = None
        if source.get("access_restrictions_notes", None):
            restrictions_notes = source["access_restrictions_notes"]

        supplied = None
        if source.get("agency_supplied", None):
            if source["agency_supplied"] == "yes":
                supplied = True 
            elif source["agency_supplied"] == "no":
                supplied = False

        retention_schedule = None
        if source.get("retention_schedule", None):
            retention_schedule = source["retention_schedule"]

        size = None
        if source.get("size", None):
            size = source["size"]

        sort_method = None
        if source.get("sort_method", None):
            size = source["sort_method"]

        source_last_updated = None
        if source.get("source_last_updated", None):
            source_last_updated = datetime.datetime.strptime(source["source_last_updated"], "%Y-%m-%d")

        source_url = None
        if source.get("source_url", None):
            source_url = source["source_url"]

        submitted_name = None
        if source.get("submitted_name", None):
            submitted_name = source["submitted_name"]

        supplying_entity = None
        if source.get("supplying_entity", None):
            supplying_entity = [e.strip for e in source["supplying_entity"]]

        update_frequency = None
        if source.get("update_frequency", None):
            update_frequency = source["update_frequency"]

        update_method = None
        if source.get("update_method", None):
            update_method = source["update_method"]

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
