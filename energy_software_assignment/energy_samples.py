import os.path


class EnergySample:
    def __init__(self, ts, partition_idx, uuid, hash_tag_vals):
        self.ts = ts
        self.partition_idx = partition_idx
        self.uuid = uuid
        self.hash_tag_vals = hash_tag_vals

    def sumHashtags(self):
        return sum(self.hash_tag_vals)


class SampleParsingException(Exception):
    pass


_HASH_TAG_NUM_MAP = {
    "#one": 1,
    "#two": 2,
    "#three": 3,
    "#four": 4,
    "#five": 5,
    "#six": 6,
    "#seven": 7,
    "#eight": 8,
    "#nine": 9,
    "#ten": 10
}


def parseHashtag(tag):
    if tag not in _HASH_TAG_NUM_MAP:
        raise SampleParsingException("Unexpected hash tag: " + tag)
    return _HASH_TAG_NUM_MAP[tag]


_COMMA_SEP = ","


def parseSample(line: str, max_partitions: int = 4) -> EnergySample:
    blocks = line.split(_COMMA_SEP)
    if len(blocks) < 4:
        raise SampleParsingException("There should be at least 4 comma seperated blocks for each sample")

    try:
        ts = int(blocks[0])
    except ValueError as ex:
        raise SampleParsingException("Expect unix timestamp but get: " + blocks[0])

    # NOTE: we are not making any assumptions about the timestamp here,
    # we probably should for real world case. For example:
    # - should the timestamp be mono increasing?
    # - should the timestamp be realistic? (i.e smaller than current time and not too old)
    # etc.

    if ts < 0:
        raise SampleParsingException("Invalid unix timestamp " + blocks[0])

    try:
        partition_idx = int(blocks[1])
    except ValueError as ex:
        raise SampleParsingException("Expect partition id but get: %s" + blocks[1])

    if partition_idx < 1 or partition_idx > max_partitions:
        raise SampleParsingException("Partition id: %s is not in range [1..%d]" % (blocks[1], max_partitions))

    # NOTE: we are not making any assumptions for the uuid format here,
    # we probably should for real world cases.
    uuid = blocks[2]
    if len(uuid) == 0:
        raise SampleParsingException("Empty uuid")

    hash_tags = []
    for tag in blocks[3:]:
        hash_tags.append(parseHashtag(tag))

    return EnergySample(ts, partition_idx, uuid, hash_tags)


class StoreException(Exception):
    pass


class SamplePartitionedStore:
    """
    Simple data store implementation that writes samples based on partition,
    opened files are guaranteed to be close once exit.
    """

    def __init__(self, store_dir: str, max_partitions: str):
        self.store_dir = store_dir
        self.max_partitions = max_partitions

    def put(self, sample: EnergySample):
        assert (1 <= sample.partition_idx <= len(self.partitions))

    def _partitionFilePath(self, partition_idx):
        return self.store_dir + ("/output-file-%d.csv" % partition_idx)

    def __enter__(self):
        self.partitions = []
        if not os.path.isdir(self.store_dir):
            raise StoreException("Directory: %s used to store files doesn't exist. Please create it before hand" % self.store_dir)

        for partition_idx in range(1, self.max_partitions + 1):
            self.partitions.append(open(self._partitionFilePath(partition_idx), 'w'))
        return self

    def __exit__(self, exc_type, exc_val, s):
        """
        Using these magic methods (__enter__, __exit__) allows you to implement objects
        which can be used easily with the with statement.
        The idea is that it makes it easy to build code which needs some 'cleandown' code executed
        (think of it as a try-finally block).
        useful example could be a database connection object (which then automagically closes the connection
        once the corresponding 'with'-statement goes out of scope)

        :param exc_type: type
        :param exc_val: value
        :param s: trace back
        :return:
        """

        failed_to_close_partitions = []
        for idx, p in enumerate(self.partitions):
            try:
                p.close()
            except Exception:
                # partition idx starts from 1
                failed_to_close_partitions.append(idx + 1)

        if failed_to_close_partitions:
            raise StoreException("Some partitions: %s are not closed successfully" % failed_to_close_partitions)




