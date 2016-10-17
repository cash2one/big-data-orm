from big_data_orm.big_query_connector.session import Session
from big_data_orm.adwords_models.adwords_keyword import AdwordsKeyword


s = Session(project_id='bigdata-141819', dataset_id='adwords_report_data',
            storage_file='/home/carlos/credentials_storage.dat')
s.connect()

print "================ Query 2 ===================="
k = AdwordsKeyword()
q = k.query(k.keyword_id, k.cost, k.keyword).order_by(k.cost, desc=True).limit(100)
print q.all(s, newest_only=True, filter_key='keyword_id')
print q.first(s, newest_only=True, filter_key='keyword_id')
