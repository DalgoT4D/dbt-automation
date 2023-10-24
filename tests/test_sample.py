import unittest
from dbt_automation.lib.dbtproject import dbtProject


class TestSimple(unittest.TestCase):
    def test_module_path(self):
        path = dbtProject.getModulePath()
        print(path)
        self.assertEqual(dbtProject.name, "test-module")


if __name__ == "__main__":
    unittest.main()
