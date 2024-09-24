import os

import psycopg2
from tqdm import tqdm
from dotenv import load_dotenv

import json

load_dotenv()
DO_DATABASE_URL = os.getenv("DO_DATABASE_URL")


def connect_digital_ocean(processed_data: list, table_name: str):
    primary_key = get_primary_key(table_name)
    headers = get_headers(processed_data, table_name)
    headers_no_id = get_headers_no_id(headers, primary_key)
    do_table_name = get_do_table_name(table_name)

    if not do_table_name:
        print("Unexpected table name!")
        return
    update_do_table(
        conflict_clause=get_conflict_clause(headers_no_id, table_name),
        headers=headers,
        primary_key=primary_key,
        processed_records=processed_data[1],
        table_name=do_table_name)


def get_primary_key(table_name):
    primary_key = {
        "Counties": "fips",
        "Link Table": "airtable_uid, agency_described_linked_uid",
        "Data Requests": "id",
    }.get(table_name, "airtable_uid")
    return primary_key


def update_do_table(conflict_clause: str, headers: list, primary_key: str, processed_records: list, table_name: str):
    print("Updating the", table_name, "table...")
    conn = psycopg2.connect(DO_DATABASE_URL)
    with conn.cursor() as curs:
        for record in tqdm(processed_records):
            clean_record = clean_records(record)
            query = f"""
                        insert into {table_name} ({', '.join(headers)}) 
                        values ({', '.join(clean_record)}) 
                        on conflict ({primary_key}) do {conflict_clause}
                    """
            curs.execute(query)
    conn.commit()
    conn.close()


def clean_records(record: list) -> list:
    clean_record = []
    for field in record:
        if type(field) in (list, dict):
            clean_field = json.dumps(field).replace("'", "''")
            clean_record.append(f"'{clean_field}'")
        elif field is None:
            clean_record.append("NULL")
        elif type(field) == str:
            clean_field = field.replace("'", "''")
            clean_record.append(f"'{clean_field}'")
        else:
            clean_record.append(f"'{field}'")
    return clean_record


def get_headers_no_id(headers, primary_key) -> list[str]:
    headers_no_id = [h for h in headers if h != primary_key]
    return headers_no_id


def get_headers(processed_data: list, table_name: str) -> list[str]:
    if table_name == "Link Table":
        headers = ["airtable_uid", "agency_described_linked_uid"]
    else:
        headers = [h for h in processed_data[0] if h != "agency_described_linked_uid"]
    return headers


def get_do_table_name(table_name: str) -> str:
    # For translating between airtable and DigitalOcean table name differences
    table_name_mapping = {
        "Counties": "counties",
        "Agencies": "agencies",
        "Data Sources": "data_sources",
        "Data Requests": "data_requests",
        "Link Table": "agency_source_link"
    }
    do_table_name = table_name_mapping.get(table_name, None)
    return do_table_name


def get_conflict_clause(headers_no_id: list, table_name: str) -> str:
    if table_name == "Link Table":
        conflict_clause = "nothing"
    else:
        set_headers = ", ".join(headers_no_id)
        excluded_headers = "(EXCLUDED." + ", EXCLUDED.".join(headers_no_id) + ")"
        conflict_clause = f"update set ({set_headers}) = {excluded_headers}"
    return conflict_clause
