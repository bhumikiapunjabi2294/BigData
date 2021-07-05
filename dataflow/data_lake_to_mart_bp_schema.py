# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" data_lake_to_mart.py demonstrates a Dataflow pipeline which reads a
large BigQuery Table, joins in another dataset, and writes its contents to a
BigQuery table.
"""


import argparse
import logging
import os
import traceback

import apache_beam as beam
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict


class DataLakeToDataMart:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept.

    This example uses side inputs to join two datasets together.
    """

    def __init__(self):
        #dir_path = os.path.dirname(os.path.realpath(__file__))
        self.schema_str = ''
        # This is the schema of the destination table in BigQuery.
        #schema_file = os.path.join(dir_path, 'resources', 'orders_denormalized.json')
        #with open(schema_file) as f:
        #    data = f.read()
            # Wrapping the schema in fields is required for the BigQuery API.
        #   self.schema_str = '{"fields": ' + data + '}'

    def add_account_details(self, row, account_details):
        """add_account_details joins two datasets together.  Dataflow passes in the
        a row from the orders dataset along with the entire account details dataset.

        This works because the entire account details dataset can be passed in memory.

        The function then looks up the account details, and adds all columns to a result
        dictionary, which will be written to BigQuery."""
        result = row.copy()
        try:
            result.update(account_details[row['EVENT_ID']])
        except KeyError as err:
            traceback.print_exc()
            logging.error("Event ID Not Found error: %s", err)
        return result


def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.   S
    # This defaults the output table in your BigQuery you'll have
    # to create the example_data dataset yourself using bq mk temp
    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='lake.orders_denormalized_sideinput_bp')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataLakeToDataMart is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_lake_to_data_mart = DataLakeToDataMart()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    #schema = parse_table_schema_from_json(data_lake_to_data_mart.schema_str)
    pipeline = beam.Pipeline(options=PipelineOptions(pipeline_args))

    # This query returns details about the account, normalized into a
    # different table.  We will be joining the data in to the main orders dataset in order
    # to create a denormalized table.
    account_details_source = (
        pipeline
        | 'Read Account Details from BigQuery ' >> beam.io.Read(
            beam.io.BigQuerySource(query=""" SELECT  * FROM `speedy-index-318415.lake.CATALOG`""",
                                   # This next stage of the pipeline maps the acct_number to a single row of
                                   # results from BigQuery.  Mapping this way helps Dataflow move your data around
                                   # to different workers.  When later stages of the pipeline run, all results from
                                   # a given account number will run on one worker.
                                   use_standard_sql=True))
        | 'Key Details' >> beam.Map(
            lambda row: (
                row['EVENT_ID'], row
            )))

    #orders_query = data_lake_to_data_mart.get_orders_query()
    (p
     # Read the orders from BigQuery.  This is the source of the pipeline.  All further
     # processing starts with rows read from the query results here.
     | 'Read Orders from BigQuery ' >> beam.io.Read(
        beam.io.BigQuerySource(query=""" SELECT  * FROM `speedy-index-318415.lake.STORM_COMBINED_DATA`""",
                                   # This next stage of the pipeline maps the acct_number to a single row of
                                   # results from BigQuery.  Mapping this way helps Dataflow move your data around
                                   # to different workers.  When later stages of the pipeline run, all results from
                                   # a given account number will run on one worker.
                                   use_standard_sql=True))
     # Here we pass in a side input, which is data that comes from outside our
     # main source.  The side input contains a map of states to their full name
     | 'Join Data with sideInput' >> beam.Map(data_lake_to_data_mart.add_account_details, AsDict(
        account_details_source))
     # This is the final stage of the pipeline, where we define the destination
     # of the data.  In this case we are writing to BigQuery.
     | 'Write Data to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output,
            # Here we use the JSON schema read in from a JSON file.
            # Specifying the schema allows the API to create the table correctly if it does not yet exist.
            #schema=schema,
            schema='BEGIN_YEARMONTH:STRING,BEGIN_DAY:STRING,BEGIN_TIM:STRING,END_YEARMONTH:STRING,END_DAY:STRING,END_TIME:STRING,EPISODE_ID:STRING,EVENT_ID:NUMERIC,STATE:STRING,STATE_FIPS:STRING,YEAR:STRING,MONTH_NAME:STRING,EVENT_TYPE:STRING,CZ_TYPE:STRING,CZ_FIPS:STRING,CZ_NAME:STRING,WFO:STRING,BEGIN_DATE_TIME:STRING,CZ_TIMEZONE:STRING,END_DATE_TIME:STRING,INJURIES_DIRECT:STRING,INJURIES_INDIRECT:STRING,DEATHS_DIRECT:STRING,DEATHS_INDIRECT:STRING,DAMAGE_PROPERTY:STRING,DAMAGE_CROPS:STRING,SOURCE:STRING,MAGNITUDE:STRING,MAGNITUDE_TYPE:STRING,FLOOD_CAUSE:STRING,CATEGORY:STRING,TOR_F_SCALE:STRING,TOR_LENGTH:STRING,TOR_WIDTH:STRING,TOR_OTHER_WFO:STRING,TOR_OTHER_CZ_STATE:STRING,TOR_OTHER_CZ_FIPS:STRING,TOR_OTHER_CZ_NAME:STRING,BEGIN_RANGE:STRING,BEGIN_AZIMUTH:STRING,BEGIN_LOCATION:STRING,END_RANGE:STRING,END_AZIMUTH:STRING,END_LOCATION:STRING,BEGIN_LAT:STRING,BEGIN_LON:STRING,END_LAT:STRING,END_LON:STRING,EPISODE_NARRATIVE:STRING,EVENT_NARRATIVE:STRING,DATA_SOURCE:STRING,id:STRING,file_name:STRING,file_index:STRING,img_type:STRING,time_utc:STRING,minute_offsets:STRING,episode_id_1:STRING,event_type_1:STRING,EVENT_ID_1:NUMERIC,llcrnrlat:STRING,llcrnrlon:STRING,urcrnrlat:STRING,urcrnrlon:STRING,proj:STRING,size_x:STRING,size_y:STRING,height_m:STRING,width_m:STRING,data_min:STRING,data_max:STRING,pct_missing:STRING',
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
