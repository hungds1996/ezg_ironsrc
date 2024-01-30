from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import logging
import argparse
import datetime
import smtplib, ssl
import traceback
import sys

from config import *

logging.basicConfig(
    filename="./logs/main.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def send_email(receiver_email, e):
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    error_message = str(e)
    traceback_message = traceback.format_exc()

    message = f"""\
    Subject: IRONSRC IMPORTING ERROR

    Error content:
    Error: {error_message}\n\nTraceback: {traceback_message}"""

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(email_config["sender"], email_config["password"])
        server.sendmail(email_config["sender"], receiver_email, message)


def get_client():
    credentials = service_account.Credentials.from_service_account_file(json_keys)
    client = bigquery.Client(credentials=credentials)
    return client


client = get_client()

# def import_logger(end_date, rows_imported):
#     rows_to_insert = [{
#         u"job": job_name,
#         u"rows_imported": rows_imported,
#         u"date": end_date
#     }]

#     client.insert_rows_json(table_id, rows_to_insert)


def get_start_end_date(report_name):
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", help="predefined start date")
    parser.add_argument("--end_date", help="predefined end date")
    args = parser.parse_args()

    if args.start_date:
        start_date = args.start_date
    else:
        start_date = get_last_date(report_name, date_column="data_date")
        if start_date == None:
            start_date = datetime.date.today() - datetime.timedelta(days=2)
            start_date = start_date.strftime("%Y-%m-%d")

    if args.end_date:
        end_date = args.end_date
    else:
        end_date = datetime.date.today() - datetime.timedelta(days=2)
        end_date = end_date.strftime("%Y-%m-%d")

    print(start_date, end_date)
    return start_date, end_date


def get_last_date(report_name, date_column="date"):
    query = "select max({}) last_date from `{}` where report_name='{}'".format(
        import_log_table, date_column, report_name
    )
    dataframe = client.query(query).result().to_dataframe()

    last_date = dataframe["last_date"].iloc[0]

    if last_date == None:
        return None
    last_date = last_date.strftime("%Y-%m-%d")

    return last_date


def make_job_config(report_name):
    if report_name == "impression_level":
        schema = [
            bigquery.SchemaField("event_timestamp", "DATETIME"),
            bigquery.SchemaField("advertising_id", "STRING"),
            bigquery.SchemaField("advertising_vendor_id", "STRING"),
            bigquery.SchemaField("user_id", "STRING"),
            bigquery.SchemaField("ad_unit", "STRING"),
            bigquery.SchemaField("ad_network", "STRING"),
            bigquery.SchemaField("instance_name", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("placement", "STRING"),
            bigquery.SchemaField("segment", "STRING"),
            bigquery.SchemaField("AB_Testing", "STRING"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("facebook_encrypted_cpm", "STRING"),
            bigquery.SchemaField("appKey", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("import_time", "DATETIME"),
        ]

    elif report_name == "user_level":
        schema = [
            bigquery.SchemaField("ad_unit", "STRING"),
            bigquery.SchemaField("advertising_id", "STRING"),
            bigquery.SchemaField("advertising_id_type", "STRING"),
            bigquery.SchemaField("user_id", "STRING"),
            bigquery.SchemaField("segment", "STRING"),
            bigquery.SchemaField("placement", "STRING"),
            bigquery.SchemaField("ad_network", "STRING"),
            bigquery.SchemaField("AB_Testing", "STRING"),
            bigquery.SchemaField("impressions", "INTEGER"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("appKey", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("import_time", "DATETIME"),
        ]

    else:
        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("appKey", "STRING"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("adUnits", "STRING"),
            bigquery.SchemaField("att", "STRING"),
            bigquery.SchemaField("idfa", "STRING"),
            bigquery.SchemaField("abTest", "STRING"),
            bigquery.SchemaField("instanceName", "STRING"),
            bigquery.SchemaField("instanceId", "STRING"),
            bigquery.SchemaField("bundleId", "STRING"),
            bigquery.SchemaField("appName", "STRING"),
            bigquery.SchemaField("providerName", "STRING"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("eCPM", "FLOAT"),
            bigquery.SchemaField("impressions", "FLOAT"),
            bigquery.SchemaField("videoCompletions", "FLOAT"),
            bigquery.SchemaField("clicks", "FLOAT"),
            bigquery.SchemaField("impressionPerEngagedSessions", "FLOAT"),
            bigquery.SchemaField("adSourceChecks", "FLOAT"),
            bigquery.SchemaField("adSourceResponses", "FLOAT"),
            bigquery.SchemaField("adSourceAvailabilityRate", "FLOAT"),
            bigquery.SchemaField("clickThroughRate", "FLOAT"),
            bigquery.SchemaField("countryCode", "STRING"),
            bigquery.SchemaField("import_time", "DATETIME"),
        ]

    job_config = bigquery.LoadJobConfig(
        schema=schema, skip_leading_rows=1, write_disposition="WRITE_APPEND"
    )

    return job_config, schema


def get_or_create_table(table_id, report_name):
    try:
        client.get_table(table_id)
    except:
        _, schema = make_job_config(report_name)
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",  # name of column to use for partitioning
            expiration_ms=None,
        )
        table = client.create_table(table)  # Make an API request.
        logger.info(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )


def import_to_bigquery(temp_file, app_name_list, df, report_name):
    job_config, _ = make_job_config(report_name)

    for app in app_name_list:
        table_id = app_info[app][report_name]
        get_or_create_table(table_id, report_name)

        row_before = client.get_table(table_id).num_rows

        app_df = df[df["appKey"] == app]
        app_df.to_csv(temp_file, index=False)

        with open(temp_file, mode="rb") as source_file:
            job = client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )
            job.result()

        row_after = client.get_table(table_id).num_rows
        row_imported = row_after - row_before

        remove_duplicate_rows(report_name, table_id)

        logger.info("Table {}: Imported {} rows".format(table_id, row_imported))
        import_logger(app_df["date"].max(), report_name, row_imported)

        return row_imported


def import_logger(data_date, report_name, rows_imported):
    # rows_to_insert = [
    #     {
    #         "data_date": data_date,
    #         "import_date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    #         "report_name": report_name,
    #         "rows_imported": rows_imported,
    #     }
    # ]
    log_df = pd.DataFrame.from_dict(
        {
            "data_date": [data_date],
            "import_date": [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            "report_name": [report_name],
            "rows_imported": [rows_imported],
        }
    )
    log_df.to_csv(".\data\import_log.csv", index=False)

    schema = [
        bigquery.SchemaField("data_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("import_date", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("report_name", "STRING"),
        bigquery.SchemaField("rows_imported", "INTEGER"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema, skip_leading_rows=1, write_disposition="WRITE_APPEND"
    )

    with open(".\data\import_log.csv", mode="rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            "data-import-409408.logs.ironsource_logs",
            job_config=job_config,
        )
        job.result()


def remove_duplicate_rows(report_name, table_id):
    if report_name == "reporting_api":
        query = """
            create or replace table `{table_id}` 
            partition by date
            as
            select * except(rn)
            from (
            SELECT  
                *,
                row_number() over(partition by date, appKey, platform, adUnits, att, idfa, abTest, instanceName, instanceId, bundleId, appName, providerName, countryCode order by import_time desc) as rn
            FROM `{table_id}` 
            )
            where rn=1
        """.format(
            table_id=table_id
        )

    elif report_name == "impression_level":
        query = """
            create or replace table `{table_id}` 
            partition by date
            as
            select * except(rn)
            from (
            SELECT  
                *,
                row_number() over(partition by event_timestamp,advertising_id, advertising_vendor_id,user_id,ad_unit,ad_network,instance_name,country,placement,segment,AB_Testing,appKey order by import_time desc) as rn
            FROM `{table_id}` 
            )
            where rn=1
        """.format(
            table_id=table_id
        )

    elif report_name == "user_level":
        query = """
            create or replace table `{table_id}` 
            partition by date
            as
            select * except(rn)
            from (
            SELECT  
                *,
                row_number() over(partition by ad_unit,advertising_id,advertising_id_type,user_id,segment,placement,ad_network,AB_Testing,appKey,date order by import_time desc) as rn
            FROM `{table_id}` 
            )
            where rn=1
        """.format(
            table_id=table_id
        )

    query_job = client.query(query)
    query_job.result()
