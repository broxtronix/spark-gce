# Copyright 2015 Michael Broxton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    packages=['spark_gce'],
    version=str(spark_gce.__version__),
    description='This script helps you create a Spark cluster on Google Compute Engine.',
    author='Michael Broxton',
    author_email='broxton@gmail.com',
    url='https://github.com/broxtronix/spark-gce',
    download_url = 'https://github.com/broxtronix/spark-gce/tarball/1.0.5',
    scripts = ['bin/spark-gce'],
    package_data = {'spark_gce': support_files},
    install_requires=['boto']
)
