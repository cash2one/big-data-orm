from setuptools import find_packages
from setuptools import setup

setup(
    name='big-data-orm',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'google-api-python-client'
    ]
)
