import argparse
import logging
import re

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class SpliLinesToWordsFn(beam.DoFn):
    OUTPUT_TAG_SHORT_WORDS = 'tag_short_words'
    OUTPUT_TAG_CHARACTER_COUNT = 'tag_character_count'

    def process(self, element):
        yield pvalue.TaggedOutput(
            self.OUTPUT_TAG_CHARACTER_COUNT, len(element))

        words = re.findall(r'[A-Za-z\']+', element)
        for word in words:
            if len(word) <= 3:
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_SHORT_WORDS, word)
            else:
                yield word


class CountWords(beam.PTransform):
    def expand(self, pcoll):
        def count_ones(word_ones):
            (word, ones) = word_ones
            return (word, sum(ones))

        def format_result(word_count):
            (word, count) = word_count
            return '%s: %s' % (word, count)

        return (pcoll
                | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
                | 'group' >> beam.GroupByKey()
                | 'count' >> beam.Map(count_ones)
                | 'format' >> beam.Map(format_result))


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        default='gs://dataflow-samples/shakespeare/kinglear.txt',
                        help='Input file to process.')
    parser.add_argument('--output',
                        default='out-mo',
                        help='Output prefix for files to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | ReadFromText(known_args.input)

        split_lines_result = (lines
                              | beam.ParDo(SpliLinesToWordsFn()).with_outputs(
                                    SpliLinesToWordsFn.OUTPUT_TAG_SHORT_WORDS,
                                    SpliLinesToWordsFn.OUTPUT_TAG_CHARACTER_COUNT,
                                    main='words'))

        words, _, _ = split_lines_result
        short_words = split_lines_result[
            SpliLinesToWordsFn.OUTPUT_TAG_SHORT_WORDS]
        character_count = split_lines_result.tag_character_count

        (character_count
         | 'pair_with_key' >> beam.Map(lambda x: ('chars_temp_key', x))
         | beam.GroupByKey()
         | 'count chars' >> beam.Map(lambda char_counts: sum(char_counts[1]))
         | 'write chars' >> WriteToText(known_args.output + '-chars'))

        (short_words
         | 'count short words' >> CountWords()
         | 'write short words' >> WriteToText(known_args.output + '-short-words'))

        (words
         | 'count words' >> CountWords()
         | 'write words' >> WriteToText(known_args.output + '-words'))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
