import argparse
import csv
import hashlib
import json
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

dict_keys = ['Transaction unique identifier', 'Price', 'Date of Transfer', 'Postcode',
             'Property Type', 'Old/New', 'Duration', 'PAON', 'SAON', 'Street', 'Locality',
             'Town/City', 'District', 'County', 'PPD Category Type', 'Record Status']

def main(argv = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest = 'input', default = '../pp-shortened.csv', help = 'Input file for the pipeline')#../pp-complete.csv
    parser.add_argument('--output', dest = 'output', default = '../pp-transformed.ndjson', help = 'Output file for the pipeline')
    known_args = parser.parse_args(argv)

    class AddPropertyID(beam.DoFn):
        def process(self, data_element):
            #Splitting data_element into an iterable list
            for line in csv.reader([data_element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
                #Assuming that address fields do not change for the same property throughout the years,
                #generating unique hash based on the adress string to use as Property ID      
                hash_property_id = hashlib.sha1(str.encode(', '.join(line[7:14] + [line[3]]))).hexdigest()                          
                #Adding keys to the data for ease of json conversion at a later point                                                                                                                                                                                                                                                
                data_element = dict(zip(dict_keys, line))                                                                                                                                                 
            yield hash_property_id, data_element

    class JSONCreate(beam.DoFn):
        def process(self, data_element):
            hash_property_id, data_element = data_element
            #Creating a nested dictionary to later convert to a properly formated json
            combined_data_json_object = {'PropertyID': hash_property_id, 'Transactions': data_element}                                                                       
            return [json.dumps(combined_data_json_object)]

    with beam.Pipeline() as p:
        pp_transformed = (
                            p
                            | "Read from Text" >> ReadFromText(known_args.input)
                            | "Add Property ID" >> beam.ParDo(AddPropertyID())
                            | "Group by ID" >> beam.GroupByKey()
                            | "Convert to JSON" >> beam.ParDo(JSONCreate())
                            | "Write to File" >> WriteToText(known_args.output)
        )

if __name__ == '__main__':
    main()