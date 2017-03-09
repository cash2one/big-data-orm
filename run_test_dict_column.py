from big_data_orm.big_query_connector.session import Session
from big_data_orm.adwords_models.adwords_keyword import AdwordsKeyword


PROJECT_ID = 'bigdata-141819'
DATASET_ID = 'adwords_data'


def build_session():
    my_session = Session(project_id=PROJECT_ID, dataset_id=DATASET_ID,
                         storage_file='/home/carlos/credentials_storage.dat')
    my_session.connect()
    return my_session


def get_data():

    my_session = build_session()

    kw = AdwordsKeyword()

    response = kw.query().\
        order_by(kw.all_conversions, desc=True).\
        filter_by_date('2017-03-06', '2017-03-06').limit(10).all(my_session)

    print response


if __name__ == '__main__':
    get_data()
