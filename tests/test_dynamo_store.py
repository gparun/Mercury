import unittest
from unittest import TestCase

from app import ActionStatus
from persistence.DynamoStore import DynamoStore


class TestDynamoStoreRemover(TestCase):
    def test_remove_empty_strings_recursively_from_dict(self):
        # ARRANGE:
        input_data = {
            'AAPL': {
                'quote': {
                    'quote1': 'data_quote1',
                    'quote2': '',
                    'quote3': 'data_quote3',
                },
                'shouldBeCleaned': {
                    'key': '',
                    'key2': tuple(),
                    'key3': [],
                    'key4': {},
                    'key5': None
                },
                'news': {
                    'title': 'news1',
                    'content': {
                        'info1': '',
                        'info2': 'info2'
                    }
                },
            }
        }

        expected_data = {
            'AAPL': {
                'quote': {
                    'quote1': 'data_quote1',
                    'quote3': 'data_quote3',
                },
                'news': {
                    'title': 'news1',
                    'content': {
                        'info2': 'info2'
                    }
                }
            }
        }

        # ACT:
        actual_data = DynamoStore().remove_empty_strings(input_data)

        # ASSERT:
        self.assertDictEqual(expected_data, actual_data.Results, msg="Expected data does not match with actual")
        self.assertEqual(ActionStatus.SUCCESS, actual_data.ActionStatus, msg="Inccorect action status was returned")

    def test_remove_empty_strings_from_dict_with_invalid_arg(self):
        # ARRANGE:
        input_data = 'not dict'

        # ACT:
        actual_data = DynamoStore().remove_empty_strings(input_data)

        # ASSERT:
        self.assertEqual(ActionStatus.ERROR, actual_data.ActionStatus, msg="Inccorect action status was returned")


if __name__ == '__main__':
    unittest.main()
