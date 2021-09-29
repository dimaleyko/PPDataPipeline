import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

def main(argv = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest = 'input', default = '../pp-shortened.csv', help = 'Input file for the pipeline') #../pp-complete.csv
    parser.add_argument('--output', dest = 'output', default = '../pp-transformed.ndjson', help = 'Output file for the pipeline')
    known_args = parser.parse_args(argv)


    with beam.Pipeline() as p:
        pp_complete = ( p | "Read From Text" >> ReadFromText(known_args.input))
        pp_complete | WriteToText(known_args.output)
        pass

if __name__ == '__main__':
    main()