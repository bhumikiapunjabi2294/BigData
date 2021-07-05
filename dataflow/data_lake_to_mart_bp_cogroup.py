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

class LeftJoin(beam.PTransform):
    """This PTransform performs a left join given source_pipeline_name, source_data,
     join_pipeline_name, join_data, common_key constructors"""

    def __init__(self, source_pipeline_name, source_data, join_pipeline_name, join_data, common_key):
        self.join_pipeline_name = join_pipeline_name
        self.source_data = source_data
        self.source_pipeline_name = source_pipeline_name
        self.join_data = join_data
        self.common_key = common_key

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, common_key):
            return data_dict[common_key], data_dict

        """This part here below starts with a python dictionary comprehension in case you 
        get lost in what is happening :-)"""
        return ({pipeline_name: pcoll | 'Convert to ({0}, object) for {1}'
                .format(self.common_key, pipeline_name)
                                >> beam.Map(_format_as_common_key_tuple, self.common_key)
                 for (pipeline_name, pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest Cogrouped' >> beam.ParDo(UnnestCoGrouped(),
                                                   self.source_pipeline_name,
                                                   self.join_pipeline_name)
                )


class UnnestCoGrouped(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, input_element, source_pipeline_name, join_pipeline_name):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline_name]
        source_dictionaries = grouped_dict[source_pipeline_name]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                yield source_dictionary
            except IndexError:  # found no join_dictionary
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
                        default='lake.orders_denormalized_sideinput_bp')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)
        
    #LeftJoin = LeftJoin()
   #schema = parse_table_schema_from_json(LeftJoin.schema_str)
    
    pipeline_options = PipelineOptions()
    p = beam.Pipeline(options=pipeline_options)
    # Create Example read Dictionary data
    source_pipeline_name = 'source_data'
    source_data = p | 'Read Orders from BigQuery ' >> beam.io.Read(
        beam.io.BigQuerySource(query=""" SELECT  * FROM `speedy-index-318415.lake.CATALOG`""",
                                   # This next stage of the pipeline maps the acct_number to a single row of
                                   # results from BigQuery.  Mapping this way helps Dataflow move your data around
                                   # to different workers.  When later stages of the pipeline run, all results from
                                   # a given account number will run on one worker.
                                   use_standard_sql=True))
    join_pipeline_name = 'join_data'
    join_data = p | 'Read Account Details from BigQuery ' >> beam.io.Read(
            beam.io.BigQuerySource(query=""" SELECT  * FROM `speedy-index-318415.lake.STORM_COMBINED_DATA`""",
                                   # This next stage of the pipeline maps the acct_number to a single row of
                                   # results from BigQuery.  Mapping this way helps Dataflow move your data around
                                   # to different workers.  When later stages of the pipeline run, all results from
                                   # a given account number will run on one worker.
                                   use_standard_sql=True))
    common_key = 'EVENT_ID'
    pipelines_dictionary = {source_pipeline_name: source_data,
                            join_pipeline_name: join_data}
    test_pipeline = (pipelines_dictionary
                     | 'Left join' >> LeftJoin(
                source_pipeline_name, source_data,
                join_pipeline_name, join_data, common_key)
                     | 'Write Data to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output,
            # Here we use the JSON schema read in from a JSON file.
            # Specifying the schema allows the API to create the table correctly if it does not yet exist.
            #schema=schema,
            # Creates the table in BigQuery if it does not yet exist.
            #schema='BEGIN_YEARMONTH:STRING,BEGIN_DAY:STRING,BEGIN_TIME:STRING,END_YEARMONTH:STRING,END_DAY:STRING,END_TIME:STRING,EPISODE_ID:STRING,EVENT_ID:NUMERIC,STATE:STRING,STATE_FIPS:STRING,YEAR:STRING,MONTH_NAME:STRING,EVENT_TYPE:STRING,CZ_TYPE:STRING,CZ_FIPS:STRING,CZ_NAME:STRING,WFO:STRING,BEGIN_DATE_TIME:STRING,CZ_TIMEZONE:STRING,END_DATE_TIME:STRING,INJURIES_DIRECT:STRING,INJURIES_INDIRECT:STRING,DEATHS_DIRECT:STRING,DEATHS_INDIRECT:STRING,DAMAGE_PROPERTY:STRING,DAMAGE_CROPS:STRING,SOURCE:STRING,MAGNITUDE:STRING,MAGNITUDE_TYPE:STRING,FLOOD_CAUSE:STRING,CATEGORY:STRING,TOR_F_SCALE:STRING,TOR_LENGTH:STRING,TOR_WIDTH:STRING,TOR_OTHER_WFO:STRING,TOR_OTHER_CZ_STATE:STRING,TOR_OTHER_CZ_FIPS:STRING,TOR_OTHER_CZ_NAME:STRING,BEGIN_RANGE:STRING,BEGIN_AZIMUTH:STRING,BEGIN_LOCATION:STRING,END_RANGE:STRING,END_AZIMUTH:STRING,END_LOCATION:STRING,BEGIN_LAT:STRING,BEGIN_LON:STRING,END_LAT:STRING,END_LON:STRING,EPISODE_NARRATIVE:STRING,EVENT_NARRATIVE:STRING,DATA_SOURCE:STRING',
            schema='BEGIN_YEARMONTH:STRING,BEGIN_DA:STRING,BEGIN_TIME:STRING,END_YEARMONTH:STRING,END_DAY:STRING,END_TIME:STRING,EPISODE_ID:STRING,EVENT_ID:NUMERIC,STATE:STRING,STATE_FIPS:STRING,YEAR:STRING,MONTH_NAME:STRING,EVENT_TYPE:STRING,CZ_TYPE:STRING,CZ_FIPS:STRING,CZ_NAME:STRING,WFO:STRING,BEGIN_DATE_TIME:STRING,CZ_TIMEZONE:STRING,END_DATE_TIME:STRING,INJURIES_DIRECT:STRING,INJURIES_INDIRECT:STRING,DEATHS_DIRECT:STRING,DEATHS_INDIRECT:STRING,DAMAGE_PROPERTY:STRING,DAMAGE_CROPS:STRING,SOURCE:STRING,MAGNITUDE:STRING,MAGNITUDE_TYPE:STRING,FLOOD_CAUSE:STRING,CATEGORY:STRING,TOR_F_SCALE:STRING,TOR_LENGTH:STRING,TOR_WIDTH:STRING,TOR_OTHER_WFO:STRING,TOR_OTHER_CZ_STATE:STRING,TOR_OTHER_CZ_FIPS:STRING,TOR_OTHER_CZ_NAME:STRING,BEGIN_RANGE:STRING,BEGIN_AZIMUTH:STRING,BEGIN_LOCATION:STRING,END_RANGE:STRING,END_AZIMUTH:STRING,END_LOCATION:STRING,BEGIN_LAT:STRING,BEGIN_LON:STRING,END_LAT:STRING,END_LON:STRING,EPISODE_NARRATIVE:STRING,EVENT_NARRATIVE:STRING,DATA_SOURCE:STRING,id:STRING,file_name:STRING,file_index:STRING,img_type:STRING,time_utc:STRING,minute_offsets:STRING,EVENT_ID_1:NUMERIC,llcrnrlat:STRING,llcrnrlon:STRING,urcrnrlat:STRING,urcrnrlon:STRING,proj:STRING,size_x:STRING,size_y:STRING,height_m:STRING,width_m:STRING,data_min:STRING,data_max:STRING,pct_missing:STRING',
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
    
