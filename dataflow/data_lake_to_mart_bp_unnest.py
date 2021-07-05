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
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
from apache_beam.pvalue import AsDict
#from custom_transforms.joins import Join

def printfn(elem):
    print(elem)

class InnerJoin(beam.PTransform):
    """Composite Transform to implement left/right/inner/outer sql-like joins on
    two PCollections. Columns to join can be a single column or multiple columns"""

    def __init__(self, left_pcol_name, left_pcol, right_pcol_name, right_pcol, join_type, join_keys):
        """
        :param left_pcol_name (str): Name of the first PCollection (left side table in a sql join)
        :param left_pcol (Pcollection): first PCollection
        :param right_pcol_name: Name of the second PCollection (right side table in a sql join)
        :param right_pcol (Pcollection): second PCollection
        :param join_type (str): how to join the two PCollections, must be one of ['left','right','inner','outer']
        :param join_keys (dict): dictionary of two (k,v) pairs, where k is pcol name and
            value is list of column(s) you want to perform join
        """

        self.right_pcol_name = right_pcol_name
        self.left_pcol = left_pcol
        self.left_pcol_name = left_pcol_name
        self.right_pcol = right_pcol
        if not isinstance(join_keys, dict):
            raise TypeError("Column names to join on should be of type dict. Provided one is {}".format(type(join_keys)))
        if not join_keys:
            raise ValueError("Column names to join on is empty. Provide atleast one value")
        elif len(join_keys.keys()) != 2 or set([left_pcol_name, right_pcol_name]) - set(join_keys.keys()):
            raise ValueError("Column names to join should be a dictionary of two (k,v) pairs, where k is pcol name and "
                             "value is list of column(s) you want to perform join")
        else:
            self.join_keys = join_keys
        join_methods = {
            "inner": UnnestInnerJoin
        }
        try:
            self.join_method = join_methods[join_type]
        except KeyError:
            raise Exception("Provided join_type is '{}'. It should be one of {}".format(join_type, join_methods.keys()))

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, join_keys):
            return [data_dict[key] for key in join_keys], data_dict

        return ({pipeline_name: pcoll
                | 'Convert to ([join_keys], elem) for {}'.format(pipeline_name)
                    >> beam.Map(_format_as_common_key_tuple, self.join_keys[pipeline_name]) for (pipeline_name, pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest CoGrouped' >> beam.ParDo(self.join_method(), self.left_pcol_name, self.right_pcol_name)
                )

class UnnestInnerJoin(beam.DoFn):
    """This DoFn class unnests the CoGroupBykey output and emits all the
    records that have matching values in both the pcols"""

    def process(self, input_element, left_pcol_name, right_pcol_name):
        """unnests the record into multiple records, similar to how an inner join output is"""
        group_key, grouped_dict = input_element
        join_dictionaries = grouped_dict[right_pcol_name]
        source_dictionaries = grouped_dict[left_pcol_name]
        if not source_dictionaries or not join_dictionaries:
            pass
        else:
            for source_dictionary in source_dictionaries:
                for join_dictionary in join_dictionaries:
                    source_dictionary.update(join_dictionary)
                    yield source_dictionary


class LogContents(beam.DoFn):
    """This DoFn class logs the content of that which it receives """

    def process(self, input_element):
        logging.info("Contents: {}".format(input_element))
        logging.info("Contents type: {}".format(type(input_element)))
        logging.info("Contents Access input_element['EVENT_ID']: {}".format(input_element['EVENT_ID']))
        return

def run(argv=None):
    """Main entry point"""
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.   S
    # This defaults the output table in your BigQuery you'll have
    # to create the example_data dataset yourself using bq mk temp
    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='lake.sideinput_bp')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)
        
    #LeftJoin = LeftJoin()
   #schema = parse_table_schema_from_json(LeftJoin.schema_str)
   # Store the CLI arguments to variables
    #project_id = known_args.project
    
    # Setup the dataflow pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'speedy-index-318415'

    p = beam.Pipeline(options=pipeline_options)
    
    # Create Example read Dictionary data
    left_pcol_name = 'p1'
    p1 = p | 'Read Orders from BigQuery ' >> beam.io.Read(
        beam.io.BigQuerySource(query=""" SELECT  * FROM `speedy-index-318415.lake.CATALOG`""",
                                   # This next stage of the pipeline maps the acct_number to a single row of
                                   # results from BigQuery.  Mapping this way helps Dataflow move your data around
                                   # to different workers.  When later stages of the pipeline run, all results from
                                   # a given account number will run on one worker.
                                   use_standard_sql=True))
    right_pcol_name = 'p2'
    p2 = p | 'Read Account Details from BigQuery ' >> beam.io.Read(
            beam.io.BigQuerySource(query=""" SELECT  * FROM `speedy-index-318415.lake.STORM_COMBINED_DATA`""",
                                   # This next stage of the pipeline maps the acct_number to a single row of
                                   # results from BigQuery.  Mapping this way helps Dataflow move your data around
                                   # to different workers.  When later stages of the pipeline run, all results from
                                   # a given account number will run on one worker.
                                   use_standard_sql=True))
    #common_key = 'EVENT_ID'
    join_keys = {left_pcol_name: ['EVENT_ID'], right_pcol_name: ['EVENT_ID']}

    pipelines_dictionary = {left_pcol_name: p1, right_pcol_name: p2}

    test_pipeline = (pipelines_dictionary
                     | 'Inner join' >> InnerJoin(
                left_pcol_name=left_pcol_name, left_pcol=p1,
                                                               right_pcol_name=right_pcol_name, right_pcol=p2,
                                                               join_type='inner', join_keys=join_keys)
                     | 'Write Data to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output,
            # Here we use the JSON schema read in from a JSON file.
            # Specifying the schema allows the API to create the table correctly if it does not yet exist.
            #schema=schema,
            # Creates the table in BigQuery if it does not yet exist.
            schema='BEGIN_YEARMONTH:STRING,BEGIN_DAY:STRING,BEGIN_TIME:STRING,END_YEARMONTH:STRING,END_DAY:STRING,END_TIME:STRING,EPISODE_ID:STRING,EVENT_ID:NUMERIC,STATE:STRING,STATE_FIPS:STRING,YEAR:STRING,MONTH_NAME:STRING,EVENT_TYPE:STRING,CZ_TYPE:STRING,CZ_FIPS:STRING,CZ_NAME:STRING,WFO:STRING,BEGIN_DATE_TIME:STRING,CZ_TIMEZONE:STRING,END_DATE_TIME:STRING,INJURIES_DIRECT:STRING,INJURIES_INDIRECT:STRING,DEATHS_DIRECT:STRING,DEATHS_INDIRECT:STRING,DAMAGE_PROPERTY:STRING,DAMAGE_CROPS:STRING,SOURCE:STRING,MAGNITUDE:STRING,MAGNITUDE_TYPE:STRING,FLOOD_CAUSE:STRING,CATEGORY:STRING,TOR_F_SCALE:STRING,TOR_LENGTH:STRING,TOR_WIDTH:STRING,TOR_OTHER_WFO:STRING,TOR_OTHER_CZ_STATE:STRING,TOR_OTHER_CZ_FIPS:STRING,TOR_OTHER_CZ_NAME:STRING,BEGIN_RANGE:STRING,BEGIN_AZIMUTH:STRING,BEGIN_LOCATION:STRING,END_RANGE:STRING,END_AZIMUTH:STRING,END_LOCATION:STRING,BEGIN_LAT:STRING,BEGIN_LON:STRING,END_LAT:STRING,END_LON:STRING,EPISODE_NARRATIVE:STRING,EVENT_NARRATIVE:STRING,DATA_SOURCE:STRING',
            #schema='BEGIN_YEARMONTH:STRING,BEGIN_DA:STRING,BEGIN_TIME:STRING,END_YEARMONTH:STRING,END_DAY:STRING,END_TIME:STRING,EPISODE_ID:STRING,EVENT_ID:NUMERIC,STATE:STRING,STATE_FIPS:STRING,YEAR:STRING,MONTH_NAME:STRING,EVENT_TYPE:STRING,CZ_TYPE:STRING,CZ_FIPS:STRING,CZ_NAME:STRING,WFO:STRING,BEGIN_DATE_TIME:STRING,CZ_TIMEZONE:STRING,END_DATE_TIME:STRING,INJURIES_DIRECT:STRING,INJURIES_INDIRECT:STRING,DEATHS_DIRECT:STRING,DEATHS_INDIRECT:STRING,DAMAGE_PROPERTY:STRING,DAMAGE_CROPS:STRING,SOURCE:STRING,MAGNITUDE:STRING,MAGNITUDE_TYPE:STRING,FLOOD_CAUSE:STRING,CATEGORY:STRING,TOR_F_SCALE:STRING,TOR_LENGTH:STRING,TOR_WIDTH:STRING,TOR_OTHER_WFO:STRING,TOR_OTHER_CZ_STATE:STRING,TOR_OTHER_CZ_FIPS:STRING,TOR_OTHER_CZ_NAME:STRING,BEGIN_RANGE:STRING,BEGIN_AZIMUTH:STRING,BEGIN_LOCATION:STRING,END_RANGE:STRING,END_AZIMUTH:STRING,END_LOCATION:STRING,BEGIN_LAT:STRING,BEGIN_LON:STRING,END_LAT:STRING,END_LON:STRING,EPISODE_NARRATIVE:STRING,EVENT_NARRATIVE:STRING,DATA_SOURCE:STRING,id:STRING,file_name:STRING,file_index:STRING,img_type:STRING,time_utc:STRING,minute_offsets:STRING,EVENT_ID_1:NUMERIC,llcrnrlat:STRING,llcrnrlon:STRING,urcrnrlat:STRING,urcrnrlon:STRING,proj:STRING,size_x:STRING,size_y:STRING,height_m:STRING,width_m:STRING,data_min:STRING,data_max:STRING,pct_missing:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
                    # | 'Log Contents' >> beam.ParDo(LogContents())
                     )                                                           



    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    
