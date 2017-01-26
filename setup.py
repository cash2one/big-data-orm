from setuptools import find_packages
from setuptools import setup

setup(
    name='big-data-orm',
    version='0.1.2',
    packages=find_packages(),
    install_requires=[
        'oauth2client < 4.0.0',
        'google-api-python-client'
    ]
)
