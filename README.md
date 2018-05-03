# subsamplinator
This is a simple tool for subsampling text files. Subsamplinator works by first memory mapping the source file, and then using the discovered byte offsets yielding the subsample of tokens from file. 

## USAGE:
* input_file is a path to input.
* number_of_output_tokens is the number of tokens you want into the output. If too many are requested, an IndexError is raised intentionally.
* token_size is the number of lines each block of output contains (For example if you are subsampling fasta files use 4.).
* rnd_seed is the random seed that is used for subsampling. This is important for guaranteeing that you get the tokens from same locations if that is relevant (Again, good example of this if you are subsampling fasta files from a paired end experiment.).
```
python3 subsamplinator.py --input_file <input> --number_of_output_tokens <# tokens> --token_size <default 4> --rnd_seed <default 123> --output_filename <filename>
```

## REQUIREMENTS:
* numpy
