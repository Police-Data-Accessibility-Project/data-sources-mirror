#!/usr/bin/env python3

# python imports
from collections import namedtuple
import csv
import os

# third-party imports
from pyairtable import Table

Sheet = namedtuple("Sheet", ['headers', 'rows'])

def get_table_data(name):
    fieldnames = get_fieldnames(name)

    data = Table(
        os.environ["AIRTABLE_KEY"],
        os.environ["AIRTABLE_BASE_ID"],
        name
    ).all(fields=fieldnames, formula=os.environ["FORMULA"])

    return Sheet(fieldnames, data)


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
    ]


def write_csv(data_package, location):
    pass




if __name__ == "__main__":
    agencies_filename = "agencies.csv"
    sources_filename = "sources.csv"
