# tap-wootric

A [Singer](https://singer.io) tap for extracting data from the Wootric
API.

## Limitations

### Creation Date Clustering

The Wootric API limits results to 50 per request and only allows
sorting by created_at. For instances where more than 50 records have
an identical created_at date and time, it is therefore impossible to
access certain data points. In this case, the bookmark is incremented
by one second and replication continues in order to avoid a crash or
infinite loop.

### Record Updates

The Wootric API does not allow you to order results by updated_at or
filter based on updated_at date. As a result, there is no way to
incrementally upsert updated records. To capture any changes in
records that have been previously updated, conduct a full replication.

## Install

Clone this repository, and then:

```bash
› python setup.py install
```

## Run

#### Run the application

```bash

tap-wootric -c config.json -s state.json

```

Where `config.json` contains the following, retrieved from the API
section of your Wootric account settings page:

```json
{
  "client_id": "a64characterstring...",
  "client_secret": "another64characterstring..."
}
```

and `state.json` is a file containing only the value of the last state
message.

---

Copyright &copy; 2017 Stitch
