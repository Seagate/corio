import unittest
from src.libs.adapter import Adapter
from src.libs.tools.warpInterface import WarpInterface
from src.libs.tools.s3benchInterface import S3benchInterface


class AdapterTest(unittest.TestCase):
    def test_warp_adapter(self):
        """list to store objects"""
        objects = []
        iotool = WarpInterface()
        adapter = Adapter(iotool, execution=iotool.run)
        objects.append(adapter)
        adapter.execute()
        self.assertEqual(True, False)  # add assertion here

    def test_s3bench_adapter(self):
        """list to store objects"""
        objects = []
        iotool = S3benchInterface()
        adapter = Adapter(iotool, execution=iotool.run)
        objects.append(adapter)
        adapter.execute()
        self.assertEqual(True, False)  # add assertion here

    def test_adapter(self):
        objects = []
        warp = WarpInterface()
        objects.append(Adapter(warp, execution=warp.run))
        s3bench = S3benchInterface()
        objects.append(Adapter(s3bench, execution=s3bench.run))
        for obj in objects:
            print("{0} is {1} IO".format(obj, obj.run()))
        self.assertEqual(True, False)  # add assertion here

    def test_adapter_interface(self):
        objects = []
        iotool = S3benchInterface()
        adapter = Adapter(iotool, execution=iotool.run)
        objects.append(adapter)
        adapter.execute()


if __name__ == '__main__':
    unittest.main()
