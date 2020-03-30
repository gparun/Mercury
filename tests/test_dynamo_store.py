import unittest
from unittest import TestCase
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
        actual_data = DynamoStore().remove_empty_strings(input_data).Results

        # ASSERT:
        self.assertDictEqual(expected_data, actual_data, msg="Expected data does not match with actual")


if __name__ == '__main__':
    unittest.main()
