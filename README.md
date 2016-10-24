# Raccoon BigData ORM  [![Build Status](https://travis-ci.org/devraccoon/big-data-orm.svg?branch=master)](https://travis-ci.org/devraccoon/big-data-orm)
ORM to collect data from Raccoon BigData.

## Install

### pip

```
pip install git+https://github.com/devraccoon/big-data-orm
```

### setup.py

``` python
setup(
    ...
    install_requires=[
        ...
        'big-data-orm',
        ...
        ],
    ...
    dependency_links=[
        ...
        'https://github.com/devraccoon/big-data-orm/tarball/master#egg=big-data-orm'
        ...
    ]
)
```

## Usage

### Models

AdWords Models:
* AdwordsAccount
* AdwordsCampaign
* AdwordsAdgroup
* AdwordsKeyword
* AdwordsAd
* AdwordsSearchTerm

### Query

#### filter(*expression*)
*filter* method allows to insert **where** clauses to SQL query.
Expressions accepted:
* column_1 == (value | other_column)
* column_1 != (value | other_column)
* column_1 > (value | other_column)
* column_1 < (value | other_column)
* column_1 >= (value | other_column)
* column_1 <= (value | other_column)
* column_1.in_([list_of_values])

Example:
``` python
query_obj = query_obj.filter(account.name == 'testing_account').filter(acccount.clicks > 1000)
```

#### order_by(*column*, *desc=True|False*)

Allows to order the query results by some column. *desc* argument is optional, if not passed, will be assumed as False.
Example:
``` python
query_obj = query_obj.order_by(account.name).order_by(account.clicks, desc=True)
```
#### limit(*int_value*)

Allows to limit the query results.
Example:
``` python
query_obj = query_obj.order_by(account.name).order_by(account.clicks, desc=True).limit(10)
```
#### all(*session*)

Send the query operation to bigquery and return all results.
Here is a code selecting some fields from adwords_account_report data with some simple filters.

``` python
from big_data_orm.big_query_connector.session import Session
from big_data_orm.adwords_models.adwords_account import AdwordsAccount

s = Session(project_id='project_id', dataset_id='dataset_id',
            storage_file='credentials_storage.dat')
s.connect()

a = AdwordsAccount()
q = a.query(a.name, a.account_id, a.clicks)

q = q.filter(a.clicks < 10).order_by(a.clicks, desc=True).limit(10)

# newest_only and filter_key are optional
# print q.all(s, newest_only=True, filter_key='account_id')
print q.all(s)
```
