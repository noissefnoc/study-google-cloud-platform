import collections
import logging
import re
import tempfile
import unittest

from apache_beam.testing.util import open_shards

import wordcount_min


class WordCountMinTest(unittest.TestCase):
    """
    Unit test for wordcount min with direct runner.
    """
    SAMPLE_TEXT = 'a b c d e a b a\n aa bb cc aa bb aa'

    def create_temp_file(self, contents):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(contents)
            return f.name

    def test_basics(self):
        temp_path = self.create_temp_file(self.SAMPLE_TEXT)
        expected_words = collections.defaultdict(int)

        for word in re.findall(r'\w+', self.SAMPLE_TEXT):
            expected_words[word] += 1

        wordcount_min.run([
            '--input=%s*' % temp_path,
            '--output=%s.result' % temp_path])

        # Parse result file and compare.
        results = []

        with open_shards(temp_path + '.result-*-of-*') as result_file:
            for line in result_file:
                match = re.search(r'([a-z]+): ([0-9]+)', line)

                if match is not None:
                    results.append((match.group(1), int(match.group(2))))

            self.assertEqual(sorted(results), sorted(expected_words.items()))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
