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

1. Clone this repository

2. Make sure you have set `PYTHONPATH` correctly for where python will install site-packages. e.g if site-packages is `/usr/local/lib/pythonX.Y/site-packages`

3. at the root directory of the project, run
```bash
python setup.py install
```
4. Once the installation succeeds, you will see the installed location in the stdout. You will need to export the path to be able to use it.

```bash
# add this line to your .bashrc or .zshrc file depending on what shell you are using
export PATH="/your/installed/path/bin:$PATH"
# for example 
# export PATH="/opt/homebrew/opt/python@3.9/Frameworks/Python.framework/Versions/3.9/bin/:$PATH"
# then you need to source it
source ~/.zshrc # or source .bashrc
```


## Run

#### Run the application

1. You will need to create two files in the project root directory to run the application: `config.json` and `state.json` 

- `config.json` contains the following, retrieved from the API section of your Wootric account settings page and a `start_date` field which specifies which date you want to start to retrieve the data.

```json
{
  "start_date": "2022-07-10",
  "client_id": "a64characterstring...",
  "client_secret": "another64characterstring..."
}
```

- `state.json` is a file containing only the value of the last state message. you can leave as empty if you don't know the last state message.
```json
// make sure you have the curly braces otherwise it would fail
{}
```

2. Then you can run the following command at the root directory of the project.
```bash

tap-wootric -c config.json -s state.json

```

---

Copyright &copy; 2017 Stitch
