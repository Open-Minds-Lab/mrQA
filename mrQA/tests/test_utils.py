from mrQA.utils import majority_attribute_values
import unittest


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




