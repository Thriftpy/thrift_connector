import os
from setuptools import setup, find_packages

# package meta info
NAME = "thrift_connector"
VERSION = "0.19"
DESCRIPTION = "Lib to connect to thrift services with pools"
AUTHOR = "Haochuan Guo"
AUTHOR_EMAIL = "guohaochuan@gmail.com"
LICENSE = "BSD"
URL = "https://github.com/eleme/thrift_connector"
KEYWORDS = "thrift connection pool"

# package contents
PACKAGES = find_packages(
    exclude=['tests.*', 'tests', 'examples.*', 'examples',
             'dev_requirements.txt'])

here = os.path.abspath(os.path.dirname(__file__))

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    license=LICENSE,
    url=URL,
    keywords=KEYWORDS,
    packages=PACKAGES,
    install_package_data=True,
    zip_safe=False,
)
