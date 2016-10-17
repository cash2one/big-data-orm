from big_data_orm.big_query_connector.session import Session
from big_data_orm.adwords_models.adwords_keyword import AdwordsKeyword


s = Session(project_id='', dataset_id='',
            storage_file='credentials_storage.dat')
s.connect()

print "================ Query 1 ===================="
k = AdwordsKeyword()
q = k.query(k.keyword_id, k.cost, k.keyword).order_by(k.cost, desc=True).limit(100)
print q.all(s, newest_only=True, filter_key='keyword_id')
print q.first(s, newest_only=True, filter_key='keyword_id')
