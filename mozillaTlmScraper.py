from google.cloud import bigquery
import pandas as pd
import logging, datetime, argparse
# import _pytimeseries

# This is a mapping of ISO 2-letter country codes to the continent
# Add "XK": "EU" for Kosovo, "CW": "NA" for Curacao, "AS": "OC" for American Samoa, 
# "SX": "NA" for Sint Maarten, "BQ": "NA" for Bonaire, Sint Eustatius, and Saba
CONTINENT_MAP = {
    "AD": "EU", "AE": "AS", "AF": "AS", "AG": "NA", "AI": "NA",
    "AL": "EU", "AM": "AS", "AO": "AF", "AQ": "AN", "AR": "SA",
    "AT": "EU", "AU": "OC", "AW": "NA", "AX": "EU", "AZ": "AS",
    "BA": "EU", "BB": "NA", "BD": "AS", "BE": "EU", "BF": "AF",
    "BG": "EU", "BH": "AS", "BI": "AF", "BJ": "AF", "BM": "NA",
    "BN": "AS", "BO": "SA", "BR": "SA", "BS": "NA", "BT": "AS",
    "BW": "AF", "BY": "EU", "BZ": "NA", "CA": "NA", "CD": "AF",
    "CF": "AF", "CG": "AF", "CH": "EU", "CI": "AF", "CK": "OC",
    "CL": "SA", "CM": "AF", "CN": "AS", "CO": "SA", "CR": "NA",
    "CU": "NA", "CV": "AF", "CY": "EU", "CZ": "EU", "DE": "EU",
    "DJ": "AF", "DK": "EU", "DM": "NA", "DO": "NA", "DZ": "AF",
    "EC": "SA", "EE": "EU", "EG": "AF", "EH": "AF", "ER": "AF",
    "ES": "EU", "ET": "AF", "FI": "EU", "FJ": "OC", "FM": "OC",
    "FO": "EU", "FR": "EU", "GA": "AF", "GB": "EU", "GD": "NA",
    "GE": "EU", "GF": "SA", "GG": "EU", "GH": "AF", "GI": "EU",
    "GL": "NA", "GM": "AF", "GN": "AF", "GP": "NA", "GQ": "AF",
    "GR": "EU", "GT": "NA", "GU": "OC", "GW": "AF", "GY": "SA",
    "HK": "AS", "HN": "NA", "HR": "EU", "HT": "NA", "HU": "EU",
    "ID": "AS", "IE": "EU", "IL": "AS", "IM": "EU", "IN": "AS",
    "IQ": "AS", "IR": "AS", "IS": "EU", "IT": "EU", "JE": "EU",
    "JM": "NA", "JO": "AS", "JP": "AS", "KE": "AF", "KG": "AS",
    "KH": "AS", "KI": "OC", "KM": "AF", "KN": "NA", "KW": "AS",
    "KY": "NA", "KZ": "AS", "LA": "AS", "LB": "AS", "LC": "NA",
    "LI": "EU", "LK": "AS", "LR": "AF", "LS": "AF", "LT": "EU",
    "LU": "EU", "LV": "EU", "LY": "AF", "MA": "AF", "MC": "EU",
    "MD": "EU", "ME": "EU", "MG": "AF", "MH": "OC", "MK": "EU",
    "ML": "AF", "MM": "AS", "MN": "AS", "MO": "AS", "MP": "OC",
    "MQ": "NA", "MR": "AF", "MS": "NA", "MT": "EU", "MU": "AF",
    "MV": "AS", "MW": "AF", "MX": "NA", "MY": "AS", "MZ": "AF",
    "NA": "AF", "NC": "OC", "NE": "AF", "NF": "OC", "NG": "AF",
    "NI": "NA", "NL": "EU", "NO": "EU", "NP": "AS", "NR": "OC",
    "NU": "OC", "NZ": "OC", "OM": "AS", "PA": "NA", "PE": "SA",
    "PF": "OC", "PG": "OC", "PH": "AS", "PK": "AS", "PL": "EU",
    "PM": "NA", "PN": "OC", "PR": "NA", "PS": "AS", "PT": "EU",
    "PW": "OC", "PY": "SA", "QA": "AS", "RE": "AF", "KR": "AS",
    "KP": "AS", "VG": "NA", "SH": "AF", "RO": "EU", "RS": "EU",
    "RU": "EU", "RW": "AF", "SA": "AS", "SB": "OC", "SC": "AF",
    "SD": "AF", "SE": "EU", "SG": "AS", "SI": "EU", "SK": "EU",
    "SL": "AF", "SM": "EU", "SN": "AF", "SO": "AF", "SR": "SA",
    "SS": "AF", "ST": "AF", "SV": "NA", "SY": "AS", "SZ": "AF",
    "TC": "NA", "TD": "AF", "TG": "AF", "TH": "AS", "TJ": "AS",
    "TK": "OC", "TL": "AS", "TM": "AS", "TN": "AF", "TO": "OC",
    "TR": "EU", "TT": "NA", "TV": "OC", "TW": "AS", "TZ": "AF",
    "UA": "EU", "UG": "AF", "US": "NA", "VI": "NA", "UY": "SA",
    "UZ": "AS", "VA": "EU", "VC": "NA", "VE": "SA", "VN": "AS",
    "VU": "OC", "WS": "OC", "YE": "AS", "YT": "AF", "ZA": "AF",
    "ZM": "AF", "ZW": "AF", "XK": "EU", "CW": "NA", "AS": "OC",
    "SX": "NA", "BQ": "NA"
}

BASEKEY="mozilla_tlm"

def transform_metrics(row, metric_columns):
    """
    Transforms the metric values in a DataFrame row based 
    on specified criteria. Specific metrics are scaled and 
    rounded to eliminate their decimal component by multiplying 
    by 10^10, and other metrics are rounded to 8 decimal places. 
    The operation preserves NaN values (i.e., does not 
    perform transformations on NaN values).

    Parameters:
    - row (pd.Series): A row from a pandas DataFrame representing 
    a single record of data. This row contains metric columns 
    whose values may need to be transformed based on their definitions.
    - metric_columns (list of str): A list of column names in the 
    row that should be checked and potentially transformed. 
    These column names are the metric identifiers.

    Returns:
    pd.Series: The modified row with transformed metrics where 
    applicable. Metrics specified in the targeted list are 
    either scaled and rounded to the nearest whole number or 
    rounded to 8 decimal places, depending on their specific 
    transformation rules.

    Transformation rules:
    - 'proportion_undefined', 'proportion_timeout', 'proportion_abort', 
    'proportion_unreachable', 'proportion_terminated', 
    'proportion_channel_open', 'missing_dns_success', 
    'missing_dns_failure', 'ssl_error_prop': Multiply 
    by 10^10 and round to the nearest integer if not NaN.
    - All other metrics in `metric_columns`: Round to 8 
    decimal places if not NaN.

    The function is designed to be used with DataFrame's apply 
    method, where each row is processed independently.
    """
    for metric in metric_columns:
        if metric in ['proportion_undefined', 'proportion_timeout', 'proportion_abort', 
                      'proportion_unreachable', 'proportion_terminated', 'proportion_channel_open', 
                      'missing_dns_success', 'missing_dns_failure', 'ssl_error_prop']:
            if pd.notna(row[metric]):
                row[metric] = round(row[metric] * 10000000000)
        else:
            if pd.notna(row[metric]):
                row[metric] = round(row[metric], 8)
    return row

def fetchData(project_id, start_time, end_time, saved):
    """ 
    Fetches Mozilla telemetry data from the Google Bigquery for a given 
    time range and saves it into a dictionary that is keyed by timestamp.
    Each dictionary item is a list of 2-tuples (key, value) where
    key is the dot-delimited string describing the series that will
    be passed into telegraf via kafka, and value is the telemetry value of a 
    particular country city pair fetched for the series at that timestamp.
    Parameters:
    - project_id (str): The ID of the Google Cloud project that 
    contains the BigQuery dataset. 
    - start_time (datetime.datetime): The start of the time range 
    for the query. This is a datetime object marking the inclusive 
    beginning of the period for which data will be fetched. 
    - end_time (datetime.datetime): The end of the time range for 
    the query. This is a datetime object marking the exclusive 
    end of the query period. 
    - saved (dict): A dictionary to store the processed results. 
    The dictionary uses UNIX timestamps as keys, and each key is 
    associated with a list of tuples. Each tuple contains a 
    metric identifier and its value, formatted as 
    (mozilla_tlm.<continent>.<country>.<city>.<metrics>, metric_value).
    Returns:
     -1 on error, 0 if no data was available, 1 if successful
    """
    # Create a BigQuery client using Application Default Credentials
    client = bigquery.Client(project=project_id)
    
    # Define the query with placeholders for the parameters
    query = """
    SELECT *
    FROM `moz-fx-data-shared-prod.internet_outages.global_outages_v1`
    WHERE datetime >= @start_time AND datetime < @end_time
    ORDER BY datetime, country, city
    --LIMIT 10
    """
    
    # Configure the query to use parameterized queries for safe substitution
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time)
        ]
    )
    
    try:
        # Execute the query
        query_job = client.query(query, job_config=job_config)
        result_df = query_job.to_dataframe()

    except bigquery.exceptions.GoogleCloudError as e:
        logging.error("Failed to get telemetry data from %s to %s: %s", str(start_time), str(end_time), str(e))
        return -1
    except Exception as e:
        logging.error("An unexpected error occurred from %s to %s: %s", str(start_time), str(end_time), str(e))
        return -1

    # Check if the result dataframe is empty
    if result_df is None:
        print("The telemetry data from %s to %s is None", str(start_time), str(end_time))
        return 0
    
    # IODA uses a "continent.country" format to hierarchically structure
    # geographic time series so we need to add the appropriate continent
    # for our requested data.    
    result_df['continent'] = result_df['country'].map(CONTINENT_MAP).fillna("Unknown")
    # Check if there are any 'Unknown' continents and log them
    unknown_countries = result_df[result_df['continent'] == "Unknown"]['country'].unique()
    if len(unknown_countries) > 0:
        logging.error("No continent mapping for: %s", ', '.join(unknown_countries))

    col_order = ['datetime', 'continent', 'country', 'city'] + [col for col in result_df.columns if col not in ['datetime', 'continent', 'country', 'city']]
    result_df = result_df[col_order]
    result_df.sort_values(by=['datetime', 'continent', 'country', 'city'], inplace=True)

    metric_columns = [col for col in result_df.columns if col not in ['datetime', 'continent', 'country', 'city']]
    # Transform metric values
    # result_df = result_df.apply(lambda row: transform_metrics(row, metric_columns), axis=1)

    # pytimeseries works best if we write all datapoints for a given timestamp
    # in a single batch, so we will save our fetched data into a dictionary
    # keyed by timestamp. Once we've fetched everything, then we can walk
    # through that dictionary to emit the data in timestamp order.
    for _, row in result_df.iterrows():
        ts = int(pd.Timestamp(row['datetime']).timestamp())
        entries = []
        for metric in metric_columns:
            key = f"{BASEKEY}.{row['continent']}.{row['country']}.{row['city']}.{metric}"
            value = row[metric]
            entries.append((key, value))
        if ts not in saved:
            saved[ts] = []
        saved[ts].extend(entries)

    return 1

def count_tuples_in_saved(saved):
    """
    Counts the total number of tuples in the 'saved' dictionary.

    Parameters:
    - saved (dict): A dictionary where each key is a timestamp and each value is a list of tuples.

    Returns:
    - int: The total number of tuples across all lists in the dictionary.
    """
    total_tuples = 0
    for key in saved:
        total_tuples += len(saved[key])
    return total_tuples

def main(args):
    datadict = {}

    # # Boiler-plate libtimeseries setup for a kafka output
    # pyts = _pytimeseries.Timeseries()
    # be = pyts.get_backend_by_name('kafka')
    # if not be:
    #     logging.error('Unable to find pytimeseries kafka backend')
    #     return -1
    # if not pyts.enable_backend(be, "-b %s -c %s -f ascii -p %s" % ( \
    #         args.broker, args.channel, args.topicprefix)):
    #     logging.error('Unable to initialise pytimeseries kafka backend')
    #     return -1

    # kp = pyts.new_keypackage(reset=False, disable=True)
    # # Boiler-plate ends

    # Determine the start and end time periods for our upcoming query
    if args.endtime:
        endtime = datetime.datetime.fromisoformat(args.endtime)   

    else:
        # Sets endtime to today's date at midnight
        endtime = datetime.datetime.combine(datetime.date.today(), datetime.time())

    if args.starttime:
        starttime = datetime.datetime.fromisoformat(args.starttime)
    else:
        starttime = endtime - datetime.timedelta(days=1)

    if (starttime > endtime):
        logging.error('Start time is after end time')
        return -1

    ret = fetchData(args.projectid, starttime, endtime, datadict)

    # for ts, dat in sorted(datadict.items()):

    #     # pytimeseries code to save each key and value for this timestamp
    #     for val in dat:
    #         idx = kp.get_key(val[0])
    #         if idx is None:
    #             idx = kp.add_key(val[0])
    #         else:
    #             kp.enable_key(idx)
    #         kp.set(idx, val[1])

    #     # Write to the kafka queue
    #     kp.flush(ts)
    print("Total tuples in saved:", count_tuples_in_saved(datadict))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
            description='Continually fetches Mozilla telemetry data from the Google Bigquery and writes it into kafka')

    # parser.add_argument("--broker", type=str, required=True, help="The kafka broker to connect to")
    # parser.add_argument("--channel", type=str, required=True, help="Kafka channel to write the data into")
    # parser.add_argument("--topicprefix", type=str, required=True, help="Topic prefix to prepend to each Kafka message")
    parser.add_argument("--projectid", type=str, required=True, help="The Google Cloud project ID")
    parser.add_argument("--starttime", type=str, help="Fetch data from this time formatted as an ISO 8601 string (e.g., \
                        \"2024-07-23T00:00:00\"), and this timestamp is inclusive. If not provided, \
                        defaults to 1 day before endtime.")
    parser.add_argument("--endtime", type=str, help="Fetch data up until this time formatted as an ISO 8601 string \
                        (e.g., \"2024-07-24T00:00:00\"), and this timestamp is exclusive. If not provided, \
                        defaults to today's date at midnight.")

    args = parser.parse_args()

    main(args)