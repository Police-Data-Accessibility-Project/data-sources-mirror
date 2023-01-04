# Data sources mirror
**PDAP metadata made easier to access and work with, beginning with data sources and agencies**

The data offered here represents an export of what PDAP and its community of volunteers collects about which law-enforcement agencies create or maintain which data.

At the moment, that means taking advantage of a `Python` wrapper around the Airtable API to provide flat but joinable data -- an agency's `airtable_uid` making that relatively straightforward.

We're also providing the script we use to do any munging or transformations. The intent is to do as little of that as we can, erring on the side of being true to what the API wrapper returns, while also being transparent about how we're doing things.

That script won't run on its own out of the box. It relies on Airtable permissions. To request access to those permissions and associated keys, ping PDAP staff. But meanwhile, the data on the other side of those permissions is pretty much what you'll find in the provided data in the `csv/` directory of this repo. Clone it down or otherwise copy those files and you'll have what we have.

These `csv`s are updated automatically every month.

Some caveats:
- As with all data, this is human-entered and human-devised and subject to imperfections. Corrections and suggestions and questions welcome.
- We'd love to have more [fields filled out, or new records added](https://docs.pdap.io/activities/share-data/contribute-data-sources), about any of these [agencies](https://airtable.com/shr43ihbyM8DDkKx4) or [data sources](https://airtable.com/shrUAtA8qYasEaepI/tblx8XaKnFTphWNQM).


