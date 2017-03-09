from big_data_orm.big_query_connector.session import Session
from big_data_orm.adwords_models.adwords_keyword import AdwordsKeyword


PROJECT_ID = ''
DATASET_ID = 'adwords_data'


def build_session():
    # Create a connection with the BigQuery sevrice
    my_session = Session(project_id=PROJECT_ID, dataset_id=DATASET_ID,
                         storage_file='credentials_storage.dat')
    my_session.connect()

    return my_session


def get_all_kw_data():

    my_session = build_session()

    kw = AdwordsKeyword()

    # If no argumnents are passed to the query() method,
    # the query result will include all KW fields.
    response = kw.query().\
        order_by(kw.total_conversion_value, desc=True).\
        filter_by_date('2017-03-07', '2017-03-07').limit(10).all(my_session)

    return response


if __name__ == '__main__':
    print get_all_kw_data()
