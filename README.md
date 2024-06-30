# Data sources mirror
**PDAP metadata made easier to access and work with, beginning with data sources and agencies.**

## What this is
This mirrors our Data Sources [what's that?](https://docs.pdap.io/activities/data-sources/what-is-a-data-source) from Airtable to our PostgreSQL database. Airtable is the entry point for our data, but using Postgres allows us to make an API that's decoupled from Airtable.

This script runs daily from the Automation Manager, using the Dockerfile and Jenkinsfile to pull the latest changes, install any requirements, and run the script from a container environment.

## Installation
### 1. Clone this repository and navigate to the root directory.

```
git clone https://github.com/Police-Data-Accessibility-Project/data-sources-mirror.git
cd data-sources-mirror
```

### 2. Create a virtual environment.

If you don't already have virtualenv, install the package:

```

pip install virtualenv

```

Then run the following command to create a virtual environment:

```

virtualenv -p python3.9 mirror_venv

```

### 3. Activate the virtual environment.

```

source mirror_venv/bin/activate

```

### 4. Install dependencies.

```

pip install -r requirements.txt

```

### 5. Create a file named .env in the same directory containing secrets for DO_DATABASE_URL, AIRTABLE_BASE_ID, and AIRTABLE_TOKEN

The app should have a DO_DATABASE_URL, AIRTABLE_BASE_ID, and AIRTABLE_TOKEN for PDAP's Data Sources [DigitalOcean](https://digitalocean.com/). Reach out to contact@pdap.io or make noise in Discord if you'd like access to these keys.

```
DO_DATABASE_URL=postgres://data_sources_app:<password>@db-postgresql-nyc3-38355-do-user-8463429-0.c.db.ondigitalocean.com:25060/defaultdb
AIRTABLE_TOKEN=<airtable_token>
AIRTABLE_BASE_ID=<airtable_base_id>
```

### 6. Run the mirror script.

```

python3 mirror.py

```


### Update frequency
Hourly

## How it works
We're taking advantage of a Python wrapper around the Airtable API to provide flat data joinable on an agency's `airtable_uid`. The script is scheduled to run via crontab on the Automation Manager droplet in DigitalOcean, on an hourly basis. On each run, the latest code from the main branch of the repo is fetched and current requirements are installed in order to make sure the script is up to date on the droplet. During execution, only rows from each table that are new or have been updated in the last two hours are fetched to update our Postgres database.

The intent is to do as little transformation as we can, erring on the side of being true to what the API wrapper returns, while also being transparent about how we're doing things.

Some caveats:
- As with all data, this is human-entered and human-devised and subject to imperfections. Corrections and suggestions and questions welcome.
- We'd love to have more [fields filled out, or new records added](https://docs.pdap.io/activities/share-data/contribute-data-sources), related to any [agencies](https://airtable.com/shr43ihbyM8DDkKx4) or [data sources](https://airtable.com/shrUAtA8qYasEaepI/tblx8XaKnFTphWNQM).
