"""Unit tests for generating sample workload files using master config"""
import copy
import unittest

import yaml

from src.commons.yaml_parser import apply_master_config


class TestMasterConfig(unittest.TestCase):
    """Test master configuration"""

    @classmethod
    def setUpClass(cls):
        cls.master_config = {}
        with open(
            "workload/master_config.yaml", "r", encoding="utf-8"
        ) as master_config:
            cls.master_config = yaml.safe_load(master_config)

    def test_wrong_parameter(self):
        """Wrong parameters scenario"""
        te_yaml = """
        test_1:
          TEST_ID: TEST-35748
          range_read: 1
          tool: s3api
          operation: bucket
        """
        test_set = yaml.safe_load(te_yaml)
        with self.assertRaises(AssertionError) as context:
            apply_master_config(test_set, self.master_config)
        self.assertIn("Wrong parameter range_read in test_1", str(context.exception))

    def test_no_parameter(self):
        """No parameters scenario"""
        te_yaml = """
        test_1:
        """
        test_set = yaml.safe_load(te_yaml)
        with self.assertRaises(AssertionError) as context:
            apply_master_config(test_set, self.master_config)
        self.assertIn("No parameter for a test_1", str(context.exception))

    def test_min_required_parameter(self):
        """Minimum required parameters scenario"""
        te_yaml = """
        test_1:
          TEST_ID: TEST-35748
        """
        test_set = yaml.safe_load(te_yaml)
        with self.assertRaises(AssertionError) as context:
            apply_master_config(test_set, self.master_config)
        self.assertIn("Minimum required parameters are missing", str(context.exception))

    def test_bucket(self):
        """Bucket workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: TEST-35748
          object_size:
            start: 0Kib
            end: 100Kib
          min_runtime: 2h
          sessions_per_node: 1
          tool: s3api
          operation: bucket
        test_2:
          TEST_ID: TEST-35749
          tool: s3api
          operation: bucket
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))
        master_config = self.master_config["s3api"][
            test_set_copy["test_2"]["operation"]
        ]
        test_set_copy["test_2"]["min_runtime"] = master_config["min_runtime"]
        test_set_copy["test_2"]["object_size"] = {"start": "0Kib", "end": "100Kib"}
        test_set_copy["test_2"]["sessions_per_node"] = master_config[
            "sessions_per_node"
        ]
        self.assertEqual(out["test_2"], test_set_copy["test_2"])
        self.assertNotEqual(id(out["test_2"]), id(test_set_copy["test_2"]))

    def test_copy_object(self):
        """Copy object workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: TEST-37246
          tool: s3api
          operation: copy_object

        test_2:
          TEST_ID: TEST-37247
          object_size:
            start: 100Kib
            end: 1Mib
          min_runtime: 2h
          sessions_per_node: 2
          tool: s3api
          operation: copy_object
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        self.assertEqual(out["test_2"], test_set_copy["test_2"])
        self.assertNotEqual(id(out["test_2"]), id(test_set_copy["test_2"]))
        master_config = self.master_config["s3api"][
            test_set_copy["test_2"]["operation"]
        ]
        test_set_copy["test_1"]["min_runtime"] = master_config["min_runtime"]
        test_set_copy["test_1"]["object_size"] = {"start": "0Kib", "end": "100Kib"}
        test_set_copy["test_1"]["sessions_per_node"] = master_config[
            "sessions_per_node"
        ]
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))

    def test_copy_object_fix_size(self):
        """Copy object with fixed size workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: TEST-37260
          min_runtime: 5h
          tool: s3api
          operation: copy_object_fix_size
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        master_config = self.master_config["s3api"][
            test_set_copy["test_1"]["operation"]
        ]
        test_set_copy["test_1"]["object_size"] = master_config["object_size"]
        test_set_copy["test_1"]["sessions_per_node"] = master_config[
            "sessions_per_node"
        ]
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))

    def test_copy_object_range_read(self):
        """Copy object with range read workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: TEST-36594
          object_size:
            start: 0Kb
            end: 100Kb
          tool: s3api
          operation: copy_object_range_read
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        master_config = self.master_config["s3api"][
            test_set_copy["test_1"]["operation"]
        ]
        test_set_copy["test_1"]["min_runtime"] = master_config["min_runtime"]
        test_set_copy["test_1"]["object_size"] = {"start": "0Kb", "end": "100Kb"}
        test_set_copy["test_1"]["sessions_per_node"] = master_config[
            "sessions_per_node"
        ]
        test_set_copy["test_1"]["range_read"] = master_config["range_read"]
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))

    def test_object_fix_size(self):
        """Object with fixed size workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: TEST-37228
          object_size:
            - 4kb
            - 8Kb
          tool: s3api
          operation: object_fix_size
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        master_config = self.master_config["s3api"][
            test_set_copy["test_1"]["operation"]
        ]
        test_set_copy["test_1"]["min_runtime"] = master_config["min_runtime"]
        test_set_copy["test_1"]["sessions_per_node"] = master_config[
            "sessions_per_node"
        ]
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))

    def test_multipart_fixed_object_size(self):
        """Multipart object with fixed size workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: TEST-37367
          tool: s3api
          operation: multipart
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        master_config = self.master_config["s3api"][
            test_set_copy["test_1"]["operation"]
        ]
        test_set_copy["test_1"]["min_runtime"] = master_config["min_runtime"]
        test_set_copy["test_1"]["sessions"] = master_config["sessions"]
        test_set_copy["test_1"]["part_range"] = master_config["part_range"]
        test_set_copy["test_1"]["object_size"] = master_config["object_size"]
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))

    def test_multipart_object_range_read(self):
        """Multipart object with range read workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: TEST-37367
          object_size: 5Gib
          tool: s3api
          operation: multipart_range_read
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        master_config = self.master_config["s3api"][
            test_set_copy["test_1"]["operation"]
        ]
        test_set_copy["test_1"]["min_runtime"] = master_config["min_runtime"]
        test_set_copy["test_1"]["sessions"] = master_config["sessions"]
        test_set_copy["test_1"]["range_read"] = master_config["range_read"]
        test_set_copy["test_1"]["part_range"] = master_config["part_range"]
        test_set_copy["test_1"]["object_size"] = "5Gib"
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))

    def test_multipart_partcopy(self):
        """Multipart object with part copy workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: TEST-37367
          tool: s3api
          object_size: 12Mb
          operation: multipart_partcopy
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        master_config = self.master_config["s3api"][
            test_set_copy["test_1"]["operation"]
        ]
        test_set_copy["test_1"]["min_runtime"] = master_config["min_runtime"]
        test_set_copy["test_1"]["sessions"] = master_config["sessions"]
        test_set_copy["test_1"]["part_range"] = master_config["part_range"]
        test_set_copy["test_1"]["object_size"] = "12Mb"
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))

    def test_multipart_partcopy_range_read(self):
        """Multipart object with part copy and range read workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: Test-12345
          tool: s3api
          min_runtime: 4h
          sessions: 5
          operation: multipart_partcopy_range_read
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        master_config = self.master_config["s3api"][
            test_set_copy["test_1"]["operation"]
        ]
        test_set_copy["test_1"]["min_runtime"] = "4h"
        test_set_copy["test_1"]["sessions"] = 5
        test_set_copy["test_1"]["part_range"] = master_config["part_range"]
        test_set_copy["test_1"]["range_read"] = master_config["range_read"]
        test_set_copy["test_1"]["object_size"] = master_config["object_size"]
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))

    def test_multipart_partcopy_random(self):
        """Multipart object with part copy and random object size workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: Test-12345
          tool: s3api
          part_range:
            start: 50
            end: 1000
          operation: multipart_partcopy_random
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        master_config = self.master_config["s3api"][
            test_set_copy["test_1"]["operation"]
        ]
        test_set_copy["test_1"]["min_runtime"] = master_config["min_runtime"]
        test_set_copy["test_1"]["sessions"] = master_config["sessions"]
        test_set_copy["test_1"]["part_range"] = {"start": 50, "end": 1000}
        test_set_copy["test_1"]["object_size"] = master_config["object_size"]
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))

    def test_multipart_random(self):
        """Multipart object with random object size workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: Test-12345
          tool: s3api
          part_range:
            start: 50
            end: 1000
          operation: multipart_random
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        master_config = self.master_config["s3api"][
            test_set_copy["test_1"]["operation"]
        ]
        test_set_copy["test_1"]["min_runtime"] = master_config["min_runtime"]
        test_set_copy["test_1"]["sessions"] = master_config["sessions"]
        test_set_copy["test_1"]["part_range"] = {"start": 50, "end": 1000}
        test_set_copy["test_1"]["object_size"] = master_config["object_size"]
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))

    def test_object_range_read(self):
        """Object with range read workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: Test-12345
          range_read: 200bytes
          tool: s3api
          operation: object_range_read
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        master_config = self.master_config["s3api"][
            test_set_copy["test_1"]["operation"]
        ]
        test_set_copy["test_1"]["min_runtime"] = master_config["min_runtime"]
        test_set_copy["test_1"]["sessions_per_node"] = master_config[
            "sessions_per_node"
        ]
        test_set_copy["test_1"]["range_read"] = "200bytes"
        test_set_copy["test_1"]["object_size"] = master_config["object_size"]
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))

    def test_object_random_size(self):
        """Object with random size workload scenario"""
        te_yaml = """
        test_1:
          TEST_ID: Test-12345
          tool: s3api
          operation: object_random_size
        """
        test_set_orig = yaml.safe_load(te_yaml)
        test_set_copy = copy.deepcopy(test_set_orig)
        out = apply_master_config(test_set_orig, self.master_config)
        master_config = self.master_config["s3api"][
            test_set_copy["test_1"]["operation"]
        ]
        test_set_copy["test_1"]["min_runtime"] = master_config["min_runtime"]
        test_set_copy["test_1"]["sessions_per_node"] = master_config[
            "sessions_per_node"
        ]
        test_set_copy["test_1"]["object_size"] = master_config["object_size"]
        self.assertEqual(out["test_1"], test_set_copy["test_1"])
        self.assertNotEqual(id(out["test_1"]), id(test_set_copy["test_1"]))


if __name__ == "__main__":
    unittest.main()
