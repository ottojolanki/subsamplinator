import unittest
import os
import numpy as np
from unittest.mock import patch
from subsamplinator import SubsampledTokenStream

TEST_FN = 'test_file_{pid}'.format(pid=os.getpid())


class TestSubsampledTokenStream(unittest.TestCase):
    def setUp(self):
        self.file_content = b'1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n'
        if os.path.isfile(TEST_FN):
            raise OSError('test file will not be created to prevent overwrite')
        else:
            with open(TEST_FN, 'w+b') as f:
                f.write(self.file_content)

    @patch.object(SubsampledTokenStream, 'scan_offsets')
    def test_get_subsampling_mask_gives_correct_length_mask(
            self, mock_scan_offsets):
        mock_scan_offsets.return_value = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        stream_full_mask = SubsampledTokenStream('file', 10)
        stream_empty_mask = SubsampledTokenStream('file', 0)
        stream_half_mask = SubsampledTokenStream('file', 5)
        self.assertEqual(sum(stream_full_mask.get_subsampling_mask()), 10)
        self.assertEqual(sum(stream_empty_mask.get_subsampling_mask()), 0)
        self.assertEqual(sum(stream_half_mask.get_subsampling_mask()), 5)

    @patch.object(SubsampledTokenStream, 'scan_offsets')
    def test_get_subsampling_mask_fails_for_too_many_tokens(
            self, mock_scan_offsets):
        mock_scan_offsets.return_value = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        with self.assertRaises(IndexError):
            SubsampledTokenStream('file', 11)

    def test_scan_offsets(self):
        streamer = SubsampledTokenStream(TEST_FN, 2)
        self.assertTrue(
            np.all(
                np.equal(streamer.offsets,
                         np.array([0, 8, 16, 27], dtype='uint64'))))

    def tearDown(self):
        os.remove(TEST_FN)
