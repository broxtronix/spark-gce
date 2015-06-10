#!/usr/bin/env python

from setuptools import setup
import spark_gce

setup(
    name='spark-gce',
    version=str(spark_gce.__version__),
    description='',
    author='Michael Broxton',
    author_email='broxton@gmail.com',
    url='https://github.com/broxtronix/spark_gce',
    packages=['spark_gce'],
    scripts = ['bin/spark-gce'],
    long_description=open('README.md').read(),
    install_requires=open('requirements.txt').read().split()
)
