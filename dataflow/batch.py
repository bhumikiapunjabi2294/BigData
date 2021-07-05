import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

PROJECT_ID = 'speedy-index-318415'
SCHEMA = 'id:STRING,file_name:STRING,file_index:INTEGER,img_type:STRING,time_utc:DATETIME,minute_offsets:STRING,episode_id:STRING,event_id:STRING,event_type:STRING,llcrnrlat:FLOAT,llcrnrlon:FLOAT,urcrnrlat:FLOAT,urcrnrlon:FLOAT,proj:STRING,size_x:INTEGER,size_y:INTEGER,height_m:FLOAT,width_m:FLOAT,data_min:FLOAT,data_max:FLOAT,pct_missing:FLOAT'
def discard_incomplete(data):
    """Filters out records that don't have an information."""
    return len(data['event_id']) > 0 and len(data['id']) > 0 and len(data['file_name']) > 0


def convert_types(data):
    """Converts string values to their appropriate type."""
    #data['event_id'] = int(data['event_id']) if 'event_id' in data else None
    data['id'] = str(data['id']) if 'id' in data else None
    data['file_name'] = str(data['file_name']) if 'file_name' in data else None
    return data

def del_unwanted_cols(data):
    """Deleting unwanted columns"""
   #del data['minute_offsets']
    del data['proj']
    del data['llcrnrlat']
    del data['llcrnrlon']
    del data['urcrnrlat']
    del data['urcrnrlon']
    return data

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions(flags=argv,
    runner='DataflowRunner',
    project='speedy-index-318415',
    temp_location='gs://speedy-index-318415/tests',
    staging_location='gs://speedy-index-318415/teststg',
    region='us-central1'))


    (p | 'ReadData' >> beam.io.ReadFromText('gs://bhumika_punjabi_assig1_bucket/CATALOG.csv', skip_header_lines =1)
       | 'Split' >> beam.Map(lambda x: x.split(','))
       | 'format to dict' >> beam.Map(lambda x: {"id": x[0], "file_name": x[1], "file_index": x[2], "img_type": x[3], "time_utc": x[4],"minute_offsets": x[5],"episode_id": x[6],"event_id": x[7],"event_type": x[8],"llcrnrlat": x[9],"llcrnrlon": x[10],"urcrnrlat": x[11],"urcrnrlon": x[12],"proj": x[13],"size_x": x[14],"size_y": x[15],"height_m": x[16],"width_m": x[17],"pct_missing": x[18]}) 
       | 'DelIncompleteData' >> beam.Filter(discard_incomplete)
     # | 'Convertypes' >> beam.Map(convert_types)
     # | 'DelUnwantedData' >> beam.Map(del_unwanted_cols)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:storm.storm_data'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
    result.wait_until_finish()

