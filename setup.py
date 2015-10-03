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
import vapor

import fnmatch
import os

support_files = []
for root, dirnames, filenames in os.walk('vapor/support_files_gce'):
    for filename in fnmatch.filter(filenames, '*'):
        support_files.append(os.path.join(root, filename)[10:])
for root, dirnames, filenames in os.walk('vapor/support_files_ec2'):
    for filename in fnmatch.filter(filenames, '*'):
        support_files.append(os.path.join(root, filename)[10:])

setup(
    name='vapor',
    packages=['vapor'],
    version=str(spark_gce.__version__),
    description='This script helps you create a Spark cluster on Google Compute Engine or Amazon EC2.',
    author='Michael Broxton',
    author_email='broxton@gmail.com',
    url='https://github.com/broxtronix/vapor',
    download_url = 'https://github.com/broxtronix/vapor/tarball/1.1.0',
    scripts = ['bin/vapor'],
    package_data = {'vapor': support_files},
    install_requires=['boto']
)
