import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class FilterTextFn(beam.DoFn):
  def __init__(self, pattern):
    super(FilterTextFn, self).__init__()
    self.pattern = pattern
    self.matched_words = Metrics.counter(self.__class__, 'matched_words')
    self.umatched_words = Metrics.counter(self.__class__, 'umatched_words')

  def process(self, element):
    word, _ = element
    if re.match(self.pattern, word):
      logging.info('Matched %s', word)
      self.matched_words.inc()
      yield element
    else:
      logging.debug('Did not match %s', word)
      self.umatched_words.inc()


class CountWords(beam.PTransform):
  def expand(self, pcoll):
    def count_ones(word_ones):
      (word, ones) = word_ones
      return (word, sum(ones))

    return (pcoll
            | 'split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                          .with_output_types(unicode))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(count_ones))


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:
    filtered_words = (
        p | 'read' >> ReadFromText(known_args.input)
        | CountWords()
        | 'FilterText' >> beam.ParDo(FilterTextFn('Flourish|stomach')))

    assert_that(
        filtered_words, equal_to([('Flourish', 3), ('stomach', 1)]))

    def format_result(word_count):
      (word, count) = word_count
      return '%s: %s' % (word, count)

    output = (filtered_words
              | 'format' >> beam.Map(format_result)
              | 'write' >> WriteToText(known_args.output))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()