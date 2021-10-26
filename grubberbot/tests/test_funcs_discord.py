import unittest

import funcs_discord as fdd


# Make sure the unittest module even loads
class TestStringMethods(unittest.TestCase):
    def test_upper(self):
        self.assertEqual("foo".upper(), "FOO")

    def test_sanity(self):
        self.assertTrue(fdd.test_unittest())


if __name__ == "__main__":
    unittest.main()
