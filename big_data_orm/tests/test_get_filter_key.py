import unittest

from big_data_orm.resources.utils.get_filter_keys import get_key


class GetFilterKeytestcase(unittest.TestCase):
    def test_get_filter_keys(self):
        table_name = 'adwords_data.adwords_account_report'
        key = get_key(table_name)
        self.assertEqual(key, 'account_id')

        table_name = 'adwords_data.adwords_campaign_report'
        key = get_key(table_name)
        self.assertEqual(key, 'campaign_id')

        table_name = 'adwords_data.adwords_adgroup_report'
        key = get_key(table_name)
        self.assertEqual(key, 'adgroup_id')

        table_name = 'adwords_data.adwords_keyword_report'
        key = get_key(table_name)
        self.assertEqual(key, 'keyword_id')

        table_name = 'adwords_data.adwords_ad_report'
        key = get_key(table_name)
        self.assertEqual(key, 'ad_id')
