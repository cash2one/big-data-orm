import logging


def get_key(table_name):
    if table_name == 'adwords_data.adwords_account_report':
        return 'account_id'
    elif table_name == 'adwords_data.adwords_campaign_report':
        return 'campaign_id'
    elif table_name == 'adwords_data.adwords_adgroup_report':
        return 'adgroup_id'
    elif table_name == 'adwords_data.adwords_ad_report':
        return 'ad_id'
    elif table_name == 'adwords_data.adwords_keyword_report':
        return 'keyword_id'
    else:
        logging.error("ORM couldnt find the right filter_key for {} ".format(table_name) +
                      "... Skiping newest only operation")
        return ''
