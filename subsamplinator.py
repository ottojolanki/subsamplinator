'''
Inspired by
https://indico.io/blog/fast-method-stream-data-from-big-data-sources/
Contains class SubsampledTokenStream that can stream N line tokens from
an arbitrarily large files. Limiting factor is the array of
memory offsets containing the start positions of tokens.
'''
import mmap
import numpy as np
from math import floor
import argparse


class SubsampledTokenStream(object):
    '''
    Stream tokens of N lines from a file. Assume that the
    total number of tokens is C, and sampling
    rate is R, then C*R tokens will be given as result,
    preserving the order of tokens.
    '''

    def __init__(self,
                 source_file,
                 number_of_output_tokens,
                 token_size=4,
                 offsets=None,
                 rnd_seed=123,
                 log_each=int(1e6)):

        np.random.seed(rnd_seed)
        self.source_file = source_file
        self.log_each = int(log_each)
        self.number_of_output_tokens = number_of_output_tokens
        self.token_size = token_size
        # offsets is a numpy.array
        if offsets:
            print('Using offsets that were given as input.')
            self.offsets = np.array(offsets, dtype='uint64')
        else:
            print('Scanning {source_file} to find byte offsets of each token.'.
                  format(source_file=self.source_file))
            self.offsets = self.scan_offsets()
        # boolean numpy array of length len(offsets)
        self.subsampling_mask = self.get_subsampling_mask()

    def __iter__(self):
        '''
        Yield subsampled tokens.
        '''
        with open(self.source_file, 'r+b') as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            subsampled_offsets = self.offsets[self.subsampling_mask]
            for position in subsampled_offsets:
                mm.seek(position)
                record = b''
                for i in range(self.token_size):
                    record += mm.readline()
                yield record

    def get_subsampling_mask(self):
        total_number_of_tokens = len(self.offsets)
        boolean_mask = np.empty(total_number_of_tokens, dtype=bool)
        number_of_included_tokens = self.number_of_output_tokens
        if number_of_included_tokens > total_number_of_tokens:
            raise IndexError('Too many tokens requested')
        else:
            boolean_mask[:number_of_included_tokens] = True
            boolean_mask[number_of_included_tokens:] = False
            np.random.shuffle(boolean_mask)
        return boolean_mask

    def scan_offsets(self):
        offsets_tmp = []
        with open(self.source_file, 'r+b') as f:
            i = 1
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            offsets_tmp.append(mm.tell())
            for line in iter(mm.readline, b''):
                if i % self.token_size == 0:
                    offsets_tmp.append(mm.tell())
                i += 1
                if i % self.log_each == 0:
                    print('{millions_of_lines} million lines scanned'.format(
                        millions_of_lines=(i / 1e6)))
        offsets = np.array(offsets_tmp, dtype='uint64')
        del offsets_tmp
        return offsets


def main(args):
    streamer = SubsampledTokenStream(
        source_file=args.input_file,
        number_of_output_tokens=args.number_of_output_tokens,
        token_size=args.token_size,
        rnd_seed=args.rnd_seed)
    with open(args.output_filename, 'w+b') as f:
        for token in streamer:
            f.write(token)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_file',
        type=str,
        required=True,
        help='path to file you want to subsample')
    parser.add_argument(
        '--number_of_output_tokens',
        type=float,
        required=True,
        help='number of output tokens you want to get')
    parser.add_argument(
        '--token_size',
        type=int,
        required=False,
        default=4,
        help='how many lines per token')
    parser.add_argument(
        '--rnd_seed',
        type=int,
        required=False,
        default=123,
        help='random seed')
    parser.add_argument(
        '--output_filename', type=str, required=True, help='output filename')
    args = parser.parse_args()
    main(args)
