# subsamplinator
This is a simple tool for subsampling text files. Subsamplinator works by first memory mapping the source file, and then using the discovered byte offsets yielding the subsample of tokens from file. 
USAGE: python3 subsamplinator.py --input_file <input> --sampling_rate <rate> --token_size <default 4> --rnd_seed <default 123> --output_filename <filename>