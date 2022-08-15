import tempfile
import unittest
import os

from energy_samples import SampleParsingException, parseSample, SamplePartitionedStore

class TestEnergySamples(unittest.TestCase):

    def test_parse_invalid_unix_timestamp(self):
        with self.assertRaises(SampleParsingException):
            parseSample("-1,2,abcde,#eight")
        with self.assertRaises(SampleParsingException):
            parseSample("not_int,2,abcde,#eight")

    def test_parse_line_no_enough_blocks(self):
        with self.assertRaises(SampleParsingException):
            parseSample("123,3,abcde")
        with self.assertRaises(SampleParsingException):
            parseSample("123,3,abcde,")

    def test_parse_invalid_partition(self):
        with self.assertRaises(SampleParsingException):
            parseSample("123,0,abcde,#eight", 4)
        with self.assertRaises(SampleParsingException):
            parseSample("123,5,abcde,#eight", 4)

    def test_parse_invalid_uuid(self):
        with self.assertRaises(SampleParsingException):
            parseSample("123,1,"",#eight", 4)

    def test_parse_invalid_hashtags(self):
        with self.assertRaises(SampleParsingException):
            parseSample("123,1,abde,#eght")

    def test_parse_sample_simple(self):
        s = parseSample("123,1,uuid,#nine")
        self.assertEqual(s.ts, 123)
        self.assertEqual(s.partition_idx, 1)
        self.assertEqual(s.uuid, "uuid")
        self.assertEqual(s.hash_tag_vals, [9])

        s = parseSample("123,1,uuid,#one,#two,#three,#four,#five,#six,#seven,#eight,#nine,#ten")
        self.assertEqual(s.ts, 123)
        self.assertEqual(s.partition_idx, 1)
        self.assertEqual(s.uuid, "uuid")
        self.assertEqual(s.hash_tag_vals, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        self.assertEqual(s.sumHashtags(), 55)

    def test_sample_partitioned_store_simple(self):
        samples = [
            "1505233687023,2,3c8f3f69-f084-4a0d-b0a7-ea183fabceef,#eight,#six,#five",
            "1505233687036,2,ead3d58b-85f3-4f54-8e1e-b0a21ae99a0d,#five,#eight,#seven",
            "1505233687037,2,345f7eb1-bf33-40c1-82a4-2f91c658803f,#two,#eight,#four,#nine,#ten,#three,#one,#seven,#six,#five",
            "1505233687037,4,fe52fa24-4527-4dfd-be87-348812e0c736,#seven,#three,#six",
            "1505233687037,4,120d688c-be5a-4e1f-a6ae-8614cb8e6af9,#ten,#seven,#eight,#one,#six,#five,#two,#four",
            "1505233687037,1,ee838bbc-503a-4999-8654-bf51c173f82a,#three,#five,#six,#ten,#nine,#four,#one,#two",
            "1505233687037,2,19624f23-9614-45b0-aa98-597112b891f9,#five,#three,#nine,#one,#ten,#six",
            "1505233687038,4,c5966068-f777-4113-af4e-6eb2e14c3005,#one,#four,#three,#eight",
            "1505233687038,4,9e1eb9c6-7691-4153-b431-74b157863b3e,#two,#five,#one,#seven,#nine,#six",
            "1505233687038,1,67e4ce28-5495-481b-88b0-ca9247b80268,#seven,#five",
            "1505233687038,3,3d71e733-be9a-4a0b-b435-4bbce1a8d090,#five,#seven,#two,#six,#one,#four,#ten",
            "1505233687039,3,4fc18980-0f65-4033-b5b5-4a64effba2f5,#nine,#one,#ten,#two,#seven,#three,#eight,#six,#four",
            "1505233687039,2,363a36ea-d984-4291-920c-550e40fb0821,#two,#eight,#five,#ten,#one,#seven",
            "1505233687039,1,580dc11a-6818-45d5-8889-4ff9854f87d4,#three",
            "1505233687039,3,2811b5f8-70d2-48ef-82d1-80b5ae8f7ec0,#five,#three,#one,#seven,#nine,#four,#eight,#two",
            "1505233687040,4,cfbb2c7f-d8f4-4859-bf9f-d98d73a21d77,#nine,#eight,#one,#ten,#seven,#five,#six",
            "1505233687043,2,4a740ab9-0893-4e4f-b769-2ea47643e13e,#eight,#three,#four,#five,#two",
            "1505233687043,3,f4a037cb-ca36-4e31-9a74-1d2504fd35a4,#ten,#eight,#three,#six,#two,#one,#nine,#five,#seven,#four",
            "1505233687044,3,c75ceb2c-1dfa-4584-8190-2dfd3da30b0f,#eight,#four,#nine,#two,#seven,#five,#six",
            "1505233687044,4,424334cc-3593-4884-bcae-b0d3458dc469,#two,#seven,#four,#six"
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            with SamplePartitionedStore(tmp_dir, 4) as store:
                for line in samples:
                    store.put(parseSample(line))

            files = os.listdir(tmp_dir)
            self.assertEqual(len(files), 4)
            self.assertEqual(sorted(files),
                             ["output-file-1.csv", "output-file-2.csv", "output-file-3.csv", "output-file-4.csv"])

            with open(os.path.join(tmp_dir, 'output-file-1.csv')) as fin:
                self.assertEqual(fin.readlines(), [
                    "1505233687037,ee838bbc-503a-4999-8654-bf51c173f82a,40\n",
                    "1505233687038,67e4ce28-5495-481b-88b0-ca9247b80268,12\n",
                    "1505233687039,580dc11a-6818-45d5-8889-4ff9854f87d4,3\n"])

            with open(os.path.join(tmp_dir, 'output-file-2.csv')) as fin:
                self.assertEqual(fin.readlines(), [
                    "1505233687023,3c8f3f69-f084-4a0d-b0a7-ea183fabceef,19\n",
                    "1505233687036,ead3d58b-85f3-4f54-8e1e-b0a21ae99a0d,20\n",
                    "1505233687037,345f7eb1-bf33-40c1-82a4-2f91c658803f,55\n",
                    "1505233687037,19624f23-9614-45b0-aa98-597112b891f9,34\n",
                    "1505233687039,363a36ea-d984-4291-920c-550e40fb0821,33\n",
                    "1505233687043,4a740ab9-0893-4e4f-b769-2ea47643e13e,22\n"])
            with open(os.path.join(tmp_dir, 'output-file-3.csv')) as fin:
                self.assertEqual(fin.readlines(), [
                    "1505233687038,3d71e733-be9a-4a0b-b435-4bbce1a8d090,35\n",
                    "1505233687039,4fc18980-0f65-4033-b5b5-4a64effba2f5,50\n",
                    "1505233687039,2811b5f8-70d2-48ef-82d1-80b5ae8f7ec0,39\n",
                    "1505233687043,f4a037cb-ca36-4e31-9a74-1d2504fd35a4,55\n",
                    "1505233687044,c75ceb2c-1dfa-4584-8190-2dfd3da30b0f,41\n"])
            with open(os.path.join(tmp_dir, 'output-file-4.csv')) as fin:
                self.assertEqual(fin.readlines(), [
                    "1505233687037,fe52fa24-4527-4dfd-be87-348812e0c736,16\n",
                    "1505233687037,120d688c-be5a-4e1f-a6ae-8614cb8e6af9,43\n",
                    "1505233687038,c5966068-f777-4113-af4e-6eb2e14c3005,16\n",
                    "1505233687038,9e1eb9c6-7691-4153-b431-74b157863b3e,30\n",
                    "1505233687040,cfbb2c7f-d8f4-4859-bf9f-d98d73a21d77,46\n",
                    "1505233687044,424334cc-3593-4884-bcae-b0d3458dc469,19\n"])

    def test_sample_partitioned_store_dir_not_exists(self):
            with self.assertRaises(StoreException):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    with SamplePartitionedStore(os.path.join(tmp_dir, "/dir_not_exists"), 4) as store:
                        pass

if __name__ == '__main__':
    unittest.main()

