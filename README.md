# Raccoon BigData ORM  

ORM to collect data from Raccoon BigData.

Branch | Status
-------|-------
**master:** | [![Build Status](https://travis-ci.org/devraccoon/big-data-orm.svg?branch=master)](https://travis-ci.org/devraccoon/big-data-orm)
**sprint:** | [![Build Status](https://travis-ci.org/devraccoon/big-data-orm.svg?branch=sprint)](https://travis-ci.org/devraccoon/big-data-orm)

## Install

### pip

```
pip install --extra-index-url https://pypi.raccoon.ag big-data-orm
```

### requirements.txt

```
--extra-index-url https://pypi.raccoon.ag

...
big-data-orm
```

#### setup.py

```python
setup(
...
install_requires=[
...
'big-data-orm',
...
],
...
dependency_links=['https://pypi.raccoon.ag/simple/big-data-orm'],
...
)
```


## Usage

For more information, please check the [Wiki](https://github.com/devraccoon/big-data-orm/wiki/BigQuery-ORM---Instructions)
