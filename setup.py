#!/usr/bin/env python

from setuptools import setup
import spark_gce

import fnmatch
import os

support_files = []
for root, dirnames, filenames in os.walk('spark_gce/support_files/templates'):
    for filename in fnmatch.filter(filenames, '*'):
        support_files.append(os.path.join(root, filename)[10:])

setup(
    name='spark-gce',
    version=str(spark_gce.__version__),
    description='',
    author='Michael Broxton',
    author_email='broxton@gmail.com',
    url='https://github.com/broxtronix/spark_gce',
    packages=['spark_gce'],
    scripts = ['spark-gce'],
    package_data = {'spark_gce': support_files},
    long_description=open('README.md').read(),
    install_requires=open('requirements.txt').read().split()
)
