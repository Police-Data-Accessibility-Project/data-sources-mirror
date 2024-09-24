import os
from collections import namedtuple

from dotenv import load_dotenv
from pyairtable import Api


def get_recent_airtable_data(table_name: str) -> list:
    data = api.table(
        AIRTABLE_BASE_ID,
        table_name
    ).all(formula=f"DATETIME_DIFF(NOW(),{AIRTABLE_TABLES[table_name]},'hours') < 2")
    return data

load_dotenv()
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
api = Api(AIRTABLE_TOKEN)


def prep_counties() -> dict:
    # For getting county fips in agencies table

    table_name = "Counties"
    counties = api.table(
        AIRTABLE_BASE_ID,
        table_name
    ).all(fields=["fips", "name", "airtable_uid"])

    # might be more we can do here to be useful
    cleaned = (
        c["fields"]
        for c in counties
    )

    results = {}

    for county in cleaned:
        try:
            results[county["airtable_uid"]] = {
                "fips": county["fips"],
                "name": county["name"] if county.get("name") is not None else "NAME_UNKNOWN",
            }
        except KeyError as e:
            raise RuntimeError(f"Missing required field in {county}: {e}")
    return results


AIRTABLE_TABLES = {
    "Counties": "airtable_county_created",
    "Agencies": "airtable_agency_last_modified",
    "Data Sources": "airtable_source_last_modified",
    "Data Requests": "date_status_last_changed"
}
Sheet = namedtuple("Sheet", ['headers', 'rows'])


def get_full_table_data(table_name: str) -> Sheet:
    print(f"getting {table_name} table data ....")
    fieldnames = get_full_fieldnames(table_name)

    data = get_recent_airtable_data(table_name)

    # ditch unneeded nesting and get to the objects we care about;
    # nothing should have to care about the original
    # structure beyond this point
    rows = (d["fields"] for d in data)
    return Sheet(fieldnames, rows)


def get_full_fieldnames(name: str) -> list:
    fieldnames_map = {
        "Agencies": agency_fieldnames_full,
        "Data Sources": source_fieldnames_full,
        "Counties": county_fieldnames_full,
        "Data Requests": requests_fieldnames_full,
    }

    if name in fieldnames_map:
        return fieldnames_map[name]()
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
        "id",
        "submission_notes",
        "request_status",
        "submitter_contact_info",
        "agency_described_submitted",
        "record_type",
        "archive_reason",
        "date_created",
        "status_last_changed"
    ]


def process_data_link_full(table_name: str, data: Sheet) -> (Sheet, Sheet):
    print(f"processing {table_name} data ....")
    processed, processed_link = process_sources_full(data.rows)
    return Sheet(data.headers, processed), Sheet(["airtable_uid", "agency_described_linked_uid"], processed_link)


def process_sources_full(data: Sheet) -> (Sheet, Sheet):
    processed = []
    processed_link = []  # for the link table

    columns = get_full_fieldnames("Data Sources")
    for source in data:

        row = []
        for field in columns:
            # For the link table:
            if field == "agency_described_linked_uid":
                agency_linked = source.get(field, None)
            elif field == "airtable_uid":
                airtable_uid = source.get(field, None)
                row.append(airtable_uid)
            else:
                row.append(source.get(field, None))

        # if there is a linked agency, save it to the link table
        if agency_linked:
            # Sometimes there are multiple linked agencies, we want to capture each one
            for i in range(len(agency_linked)):
                link_row = [airtable_uid, agency_linked[i]]
                processed_link.append(link_row)

        processed.append(row)
    return processed, processed_link


def process_data_full(table_name: str, data: Sheet) -> Sheet:
    print(f"processing {table_name} data ....")
    if table_name == "Agencies":
        processed = process_agencies_full(data.rows)
    elif table_name in ("Counties", "Data Requests"):
        processed = process_standard_full(table_name, data.rows)
    else:
        raise RuntimeError("Check the table name -- it might not be accurate")
    return Sheet(data.headers, processed)


def process_agencies_full(data: list) -> list:
    processed = []

    # doing this here because we only need to do it for agencies and
    # only want to do it after we know there's agencies data
    # (get counties fips codes from their airtable uids)
    counties = prep_counties()

    for agency in data:

        columns = get_full_fieldnames("Agencies")
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


def process_county(column: str, agency: str, counties: dict) -> str:
    encoded_fips = agency.get(column, None)
    decoded_fips = None
    if encoded_fips and isinstance(encoded_fips, list) and len(encoded_fips) > 0:
        encoded_fips_popped = encoded_fips[0]
        cfips = counties.get(encoded_fips_popped, None)

        if cfips:
            decoded_fips = cfips["fips"]
    return decoded_fips


def process_county_uid(column: str, agency: dict) -> str:
    # get the string rep, it's in a list of one
    if county_airtable_uid := agency.get(column, None):
        return county_airtable_uid[0]


def process_standard_full(table_name: str, data: list) -> list:
    processed = []
    columns = get_full_fieldnames(table_name)
    for source in data:
        row = []
        for field in columns:
            row.append(source.get(field, None))
        processed.append(row)
    return processed
