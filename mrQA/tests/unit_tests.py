from mrQA.utils import majority_attribute_values, files_modified_since
from mrQA.utils import list2txt, txt2list
import unittest
import tempfile
from pathlib import Path
from MRdataset.utils import random_name
from datetime import datetime, timezone


class TestMajorityAttributeValues(unittest.TestCase):
    def test_single_majority(self):
        animals = [
            {'species': 'lion', 'color': 'orange', 'habitat': 'savanna'},
            {'species': 'tiger', 'color': 'orange', 'habitat': 'jungle'},
            {'species': 'lion', 'color': 'yellow', 'habitat': 'jungle'},
            {'species': 'cheetah', 'color': 'brown', 'habitat': 'savanna'},
            {'species': 'lion', 'color': 'orange', 'habitat': 'jungle'},
            {'species': 'puma', 'color': 'yellow', 'habitat': 'city'},
            {'species': 'cougar', 'color': 'orange', 'habitat': 'jungle'},
            {'species': 'lion', 'color': 'yellow', 'habitat': 'savanna'},
            {'species': 'panther', 'color': 'orange', 'habitat': 'jungle'},
            {'species': 'lion', 'color': 'brown', 'habitat': 'savanna'},
        ]
        maj_attr_vals = majority_attribute_values(animals)
        self.assertEqual(maj_attr_vals['species'], 'lion')
        self.assertEqual(maj_attr_vals['habitat'], 'jungle')
        self.assertEqual(maj_attr_vals['color'], 'orange')

    def test_empty_list(self):
        animals = []
        with self.assertRaises(ValueError):
            majority_attribute_values(animals)

    def test_list_empty_dicts(self):
        animals = [{}, {}, {}, {}]
        with self.assertRaises(ValueError):
            majority_attribute_values(animals)

    def test_equal_count(self):
        animals = [
            {'species': 'lion', 'color': 'orange', 'habitat': 'savanna'},
            {'species': 'tiger', 'color': 'orange', 'habitat': 'jungle'},
            {'species': 'lion', 'color': 'yellow', 'habitat': 'jungle'},
            {'species': 'tiger', 'color': 'brown', 'habitat': 'savanna'},
            {'species': 'lion', 'color': 'orange', 'habitat': 'jungle'},
            {'species': 'tiger', 'color': 'yellow', 'habitat': 'city'},
            {'species': 'tiger', 'color': 'orange', 'habitat': 'jungle'},
            {'species': 'lion', 'color': 'yellow', 'habitat': 'savanna'},
            {'species': 'tiger', 'color': 'orange', 'habitat': 'jungle'},
            {'species': 'lion', 'color': 'brown', 'habitat': 'savanna'},
        ]
        maj_attr_vals = majority_attribute_values(animals)
        self.assertIsNone(maj_attr_vals['species'])
        self.assertEqual(maj_attr_vals['habitat'], 'jungle')
        self.assertEqual(maj_attr_vals['color'], 'orange')

    def test_single_key_equal_count(self):
        animals = [
            {'species': 'lion'},
            {'species': 'tiger'},
            {'species': 'lion'},
            {'species': 'tiger'},
            {'species': 'lion'},
            {'species': 'tiger'},
            {'species': 'tiger'},
            {'species': 'lion'},
            {'species': 'tiger'},
            {'species': 'lion'},
        ]
        maj_attr_vals = majority_attribute_values(animals)
        self.assertIsNone(maj_attr_vals['species'])

    def test_single_key_majority(self):
        animals = [
            {'species': 'lion'},
            {'species': 'lion'},
            {'species': 'lion'},
            {'species': 'tiger'},
            {'species': 'lion'},
            {'species': 'tiger'},
            {'species': 'tiger'},
            {'species': 'lion'},
            {'species': 'tiger'},
            {'species': 'lion'},
        ]
        maj_attr_vals = majority_attribute_values(animals)
        self.assertEqual(maj_attr_vals['species'], 'lion')

    def test_none_majority(self):
        animals = [
            {'species': None},
            {'species': None},
            {'species': None},
            {'species': 'tiger'},
            {'species': None},
            {'species': 'tiger'},
            {'species': 'tiger'},
            {'species': None},
            {'species': 'tiger'},
            {'species': None},
        ]
        maj_attr_vals = majority_attribute_values(animals)
        self.assertEqual(maj_attr_vals['species'], 'tiger')

    def test_all_none(self):
        animals = [
            {'species': None},
            {'species': None},
            {'species': None},
            {'species': None},
            {'species': None},
            {'species': None},
            {'species': None},
            {'species': None},
            {'species': None},
            {'species': None},
        ]
        maj_attr_vals = majority_attribute_values(animals)
        self.assertIsNone(maj_attr_vals['species'])

    def test_many_values_for_majority(self):
        animals = [
            {'species': 'lion'},
            {'species': 'lion'},
            {'species': 'lion'},
            {'species': 'tiger'},
            {'species': 'tiger'},
            {'species': 'tiger'},
            {'species': 'cheetah'},
            {'species': 'cheetah'},
            {'species': 'cheetah'},
        ]
        maj_attr_vals = majority_attribute_values(animals)
        self.assertIsNone(maj_attr_vals['species'])

    def test_length_less_than_3(self):
        animals = [
            {'species': 'lion', 'color': 'orange', 'habitat': 'savanna'},
        ]
        maj_attr_vals = majority_attribute_values(animals)
        self.assertIsNone(maj_attr_vals['species'])
        self.assertIsNone(maj_attr_vals['habitat'])
        self.assertIsNone(maj_attr_vals['color'])

        animals = [
            {'species': 'lion', 'color': 'orange', 'habitat': 'savanna'},
            {'species': 'tiger', 'color': 'yellow', 'habitat': 'jungle'},
        ]
        maj_attr_vals = majority_attribute_values(animals)
        self.assertIsNone(maj_attr_vals['species'])
        self.assertIsNone(maj_attr_vals['habitat'])
        self.assertIsNone(maj_attr_vals['color'])

    def test_none(self):
        with self.assertRaises(ValueError):
            majority_attribute_values(None)

    def test_different_keys(self):
        animals = [
            {'species': 'lion', 'color': 'orange'},
            {'species': 'tiger', 'color': 'orange'},
            {'species': 'lion', 'color': 'yellow'},
            {'species': 'tiger', 'color': 'brown', 'habitat': 'savanna'},
            {'species': 'lion', 'color': 'orange', 'habitat': 'savanna'},
            {'species': 'tiger', 'color': 'yellow', 'habitat': 'city'},
            {'species': 'tiger', 'color': 'orange', 'habitat': 'jungle'},
            {'species': 'jaguar', 'color': 'yellow', 'habitat': 'savanna'},
            {'species': 'tiger', 'color': 'orange', 'habitat': 'jungle'},
            {'species': 'jaguar', 'color': 'brown', 'habitat': 'savanna'},
        ]
        maj_attr_vals = majority_attribute_values(animals)
        self.assertEqual(maj_attr_vals['species'], 'tiger')
        self.assertEqual(maj_attr_vals['habitat'], 'savanna')
        self.assertEqual(maj_attr_vals['color'], 'orange')


class TestTxt2List(unittest.TestCase):
    def test_valid_file(self):
        file_path = Path('/tmp/tests/test.txt').resolve()
        expected = ['line1', 'line2', 'line3']
        list2txt(file_path, expected)
        actual = txt2list(file_path)
        self.assertEqual(expected, actual)

    def test_invalid_file(self):
        file_path = 'tests/test_files/invalid.txt'
        with self.assertRaises(FileNotFoundError):
            txt2list(file_path)


class TestModifiedTimeDirectory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # super().__init__()
        cls.temp_ds = cls.create_dummy_directory()

    def test_invalid_path(self):
        with self.assertRaises(FileNotFoundError):
            now = datetime.now(timezone.utc)
            files_modified_since('/tmp/mrqa', now.strftime("%m/%d/%Y %H:%M:%S"))

    @staticmethod
    def create_dummy_directory():
        temp_dir = Path(tempfile.gettempdir())
        temp_folder_name = temp_dir / random_name()
        temp_folder_name.mkdir(parents=True, exist_ok=True)

        num_folder = 10
        num_files = 5
        for i in range(num_folder):
            temp_subdir_path = temp_folder_name / random_name()
            temp_subdir_path.mkdir(parents=True, exist_ok=True)
            for j in range(num_files):
                tmp = tempfile.NamedTemporaryFile(delete=False,
                                                  dir=temp_subdir_path)
        return temp_folder_name

    def test_basic(self):
        now = datetime(2023, 2, 5, 18, 00)
        # datetime.now(timezone.utc)
        date_time = now.strftime("%m/%d/%Y %H:%M:%S")
        valid_files = files_modified_since(self.temp_ds, date_time, '/tmp/',
                                           time_format='datetime')
        print(valid_files)


if __name__ == '__main__':
    a = TestModifiedTimeDirectory()
    # a.check_basic()



