from setuptools import find_packes
from setuptools import setup

setup(
    name='big-data-orm',
    version='0.1',
    packages=find_packes()
    install_requires=[
        'google-api-python-client'
    ]
)
