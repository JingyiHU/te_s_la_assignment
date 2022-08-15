import argparse
from energy_samples import SamplePartitionedStore, parseSample

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="energy software assignment")
    parser.add_argument("--input-file", help="Input file contains comma separated samples", required=True)
    parser.add_argument("--output-dir", help="Target directory for the partitioned output files(assuming existence", required=True)
    # nargs: number of arguments, '?': means a single arg, can be optional
    parser.add_argument("--max-partitions", help="Max number of partitions[default=4]", nargs='?', type=int, default=4)

    args = vars(parser.parse_args())

    with open(args['input-file'], 'r') as fin:
        with SamplePartitionedStore(args['output_dir'], args['max_partitions']) as store:
            for line in fin:
                sample = parseSample(line.strip())
                store.put(sample)
