# Data sources mirror
**PDAP metadata made easier to access and work with, beginning with data sources and agencies.**

## What this is
The data offered in the `csv` and `json` directories represents an export of the Data Sources being tracked by PDAP and its community of volunteers. It can help you answer the question, "which data is available about a particular police agency?"

[Learn more about Data Sources](https://docs.pdap.io/activities/data-sources/what-is-a-data-source)
[See this same data but in Airtable](https://airtable.com/shrUAtA8qYasEaepI)

### Update frequency
Weekly

## How it works
At the moment, making such an export means taking advantage of a `Python` wrapper around the Airtable API to provide flat but joinable data -- an agency's `airtable_uid` making that relatively straightforward.

We're also providing the script we use to do any munging or transformations. The intent is to do as little of that as we can, erring on the side of being true to what the API wrapper returns, while also being transparent about how we're doing things.

That script won't run on its own out of the box. It relies on Airtable permissions. To request access to those permissions and associated keys, ping PDAP staff. But meanwhile, the data on the other side of those permissions is pretty much what you'll find in the provided data in the `csv` and `json` directories of this repo. Clone it down or otherwise copy those files and you'll have what we have.

These `csv` and `json` files are updated automatically at the same time. The difference between them, beyond simply format, is that the nestable nature of `json` means the associated agency is included with each relevant data source.

Some caveats:
- As with all data, this is human-entered and human-devised and subject to imperfections. Corrections and suggestions and questions welcome.
- We'd love to have more [fields filled out, or new records added](https://docs.pdap.io/activities/share-data/contribute-data-sources), related to any [agencies](https://airtable.com/shr43ihbyM8DDkKx4) or [data sources](https://airtable.com/shrUAtA8qYasEaepI/tblx8XaKnFTphWNQM).

## How you can help
As mentioned above, you can edit and add metadata for others to use.

You can also:
- Learn more about what PDAP considers a [`Data Source`](https://docs.pdap.io/activities/data-sources/what-is-a-data-source)
- Tell other data users about [records you've found or collected](https://docs.pdap.io/activities/share-data/contribute-data-sources#submit-data-youve-collected).
- Request help locating or better accessing [data you are looking to find](https://docs.pdap.io/activities/data-sources/request-data).
- Join [our Discord](https://discord.com/invite/cn2ZpVTdw7)
- [Donate or otherwise contribute](https://pdap.io/contribute.html). PDAP is a 501(c)3 not-for-profit organization
