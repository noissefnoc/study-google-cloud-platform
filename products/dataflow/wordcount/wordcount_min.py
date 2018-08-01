import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def run(argv=None):
    """
    Main entry point. defines and runs the wordcount pipeline.
    :param argv: None
    :return: None
    """
    # set arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://dataflow-samples/shakespeare/kinglear.txt',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        default='wc',
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # optional arguments?
    pipeline_args.extend([
        '--runner=DirectRunner',
        '--job_name=wordcount-min'
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # pipeline block
    with beam.Pipeline(options=pipeline_options) as p:
        # read input text to pipeline
        lines = p | ReadFromText(known_args.input)

        # transform input to count
        counts = (
            lines
            | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                          .with_output_types(unicode))
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum))

        def format_result(word_count):
            (word, count) = word_count
            return '%s: %s' % (word, count)

        # formatting
        output = counts | 'Format' >> beam.Map(format_result)

        # output to text
        output | WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
