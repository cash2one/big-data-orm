from big_data_orm.big_query_connector.session import Session
from big_data_orm.adwords_models.adwords_account import AdwordsAccount

s = Session(project_id='bigdata-141819', dataset_id='adwords_report_data',
            storage_file='/home/carlos/credentials_storage.dat')
s.connect()

a = AdwordsAccount()
q = a.query(a.name, a.account_id, a.clicks)

q = q.filter(a.clicks > 100).order_by(a.clicks, desc=True).limit(10)

print q.all(s, newest_only=True, filter_key='account_id')
