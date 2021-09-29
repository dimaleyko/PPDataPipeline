import argparse
import apache_beam as beam

def main(argv = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest = 'input', default = '../pp-complete.csv', help = 'Input file for the pipeline')
    parser.add_argument('--output', dest = 'output', default = '../pp-transformed.ndjson', help = 'Output file for the pipeline')
    known_args = parser.parse_args(argv)


    with beam.Pipeline() as p:
        pass

if __name__ == '__main__':
    main()