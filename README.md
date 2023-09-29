# Data sources mirror
**PDAP metadata made easier to access and work with, beginning with data sources and agencies.**

## What this is
This mirrors our Data Sources [what's that?](https://docs.pdap.io/activities/data-sources/what-is-a-data-source) from Airtable to our PostgreSQL database. Airtable is the entry point for our data, but using Postgres allows us to make an API that's decoupled from Airtable.

### Update frequency
Weekly

## How it works
We're taking advantage of a Python wrapper around the Airtable API to provide flat data joinable on an agency's `airtable_uid`.

The intent is to do as little transformation as we can, erring on the side of being true to what the API wrapper returns, while also being transparent about how we're doing things.

Some caveats:
- As with all data, this is human-entered and human-devised and subject to imperfections. Corrections and suggestions and questions welcome.
- We'd love to have more [fields filled out, or new records added](https://docs.pdap.io/activities/share-data/contribute-data-sources), related to any [agencies](https://airtable.com/shr43ihbyM8DDkKx4) or [data sources](https://airtable.com/shrUAtA8qYasEaepI/tblx8XaKnFTphWNQM).
