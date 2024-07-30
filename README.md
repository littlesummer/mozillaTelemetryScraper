## Mozilla Telemetry data scraper

This script uses the Python Client for Google BigQuery 
(https://cloud.google.com/python/docs/reference/bigquery/latest/index.html) 
to fetch Mozilla telemetry data stored in Google BigQuery 
and write it into a kafka pipe in a format that is compatible 
with the data collection backend used by IODA.

### Dependencies
 * google-cloud-bigquery -- Install by pip install google-cloud-bigquery running 
 pip install google-cloud-bigquery (https://cloud.google.com/python/docs/reference/bigquery/latest)
 * gcloud CLI -- Install by following the instructions at https://cloud.google.com/sdk/docs/install#deb
 * pandas -- Install by running pip install pandas
 * db-dtypes -- Install by running pip install db-dtypes
 * pytimeseries -- Build from source at https://github.com/CAIDA/pytimeseries
 * Optional: google-cloud-bigquery-storage -- Install by 
 running pip install google-cloud-bigquery-storage
 
### Before Running

We need to set up Application Default Credentials (ADC) for running BigQuery Queries locally. 
First make sure gcloud CLI has been installed. Then running ```gcloud auth application-default login``` 
to configure ADC (https://cloud.google.com/docs/authentication/provide-credentials-adc).

### Usage
```
usage: mozillaTlmScraper.py [-h] --broker BROKER --channel CHANNEL --topicprefix
                        TOPICPREFIX [--projectid PROJECTID]
                        [--starttime STARTTIME] [--endtime ENDTIME]

required arguments:
  --broker BROKER       The kafka broker to connect to
  --channel CHANNEL     Kafka channel to write the data into
  --topicprefix TOPICPREFIX Topic prefix to prepend to each Kafka message
  --projectid PROJECTID The Google Cloud project ID
optional arguments:
  -h, --help            show this help message and exit
  --starttime STARTTIME Fetch data from this time formatted as an ISO 8601 string (e.g., "2024-07-23T00:00:00"), and this timestamp is inclusive. If not provided, defaults to 1 day before endtime.
  --endtime ENDTIME     Fetch data up until this time formatted as an ISO 8601 string (e.g., "2024-07-24T00:00:00"), and this timestamp is exclusive. If not provided, defaults to today's date at midnight (00:00:00 UTC).
```

### Kafka Message Format

The fetched data is inserted into Kafka using the graphite text format, as
follows:

```
<KEY> <VALUE> <TIMESTAMP>
```

The key is a dot-separated series of terms that describes the particular data
series that the given data point belongs to, constructed using the following
schema:

```
mozilla_tlm.<CONTINENT_CODE>.<COUNTRY_CODE>.<CITY_NAME>.<METRIC_NAME>
```

Continent and country codes are the official 2-letter ISO codes for those
regions. 

For example, the proportion_timeout value for Atlanta, USA will have the key
`mozilla_tlm.NA.US.Atlanta.proportion_timeout`.



