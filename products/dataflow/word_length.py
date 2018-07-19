#!/usr/bin/env python

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class ExtractWordsFn(beam.DoFn):
    def process(self, element):
        print(element)


if __name__ == '__main__':
    # create pipeline
    p = beam.Pipeline(options=PipelineOptions())

    # set input pipeline
    lines = (p
             | beam.Create([
                'London Bridge is falling down,',
                'Falling down, falling down.',
                'London Bridge is falling down,',
                'My fair lady.',
                'London Bridge is broken down,',
                'Broken down, broken down.',
                'London Bridge is broken down,',
                'My fair lady.',]))

    # set process
    word_length = lines | beam.Map(len)

    # output count to stdout
    p_end = word_length | beam.ParDo(ExtractWordsFn())
    p.run()
