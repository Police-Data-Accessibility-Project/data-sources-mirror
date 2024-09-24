from dotenv import load_dotenv

from airtable_logic import AIRTABLE_TABLES, get_full_table_data, \
    process_data_link_full, process_data_full
from database_logic import connect_digital_ocean

load_dotenv()


# Functions for writing to DigitalOcean.
def full_mirror_to_digital_ocean():
    # Get the data from Airtable, process it, upload to DigitalOcean.
    for table in AIRTABLE_TABLES.keys():
        data = get_full_table_data(table)

        # TODO: Add section that continues if data is empty.

        # If data sources table, need to handle separately for link table:
        if table == "Data Sources":
            processed, processed_link = process_data_link_full(table, data)
            connect_digital_ocean(processed, table)
            # This is also where we handle the link table
            connect_digital_ocean(processed_link, "Link Table")

        else:
            processed = process_data_full(table, data)
            connect_digital_ocean(processed, table)


# handling specific cases


if __name__ == "__main__":
    full_mirror_to_digital_ocean()
