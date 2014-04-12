# Copyright 2014 TellApart, Inc.
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

import setuptools

requires = [
    'cython >= 0.13',
    'gevent == 0.13.1',
    'python-cjson >= 1.0.5',
    'redis >= 2.9',
    'requests >= 1.2.0']

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Natural Language :: English',
    'Operating System :: POSIX', ]

with open('README.md') as f:
  description = f.read()

dist = setuptools.setup(
    name='taba',
    version='0.3.3',
    long_description=description,
    license='Apache License, Version 2.0',
    url='http://github.com/tellapart/taba',
    classifiers=CLASSIFIERS,
    author='Kevin Ballard',
    author_email='kevin@tellapart.com',
    packages=setuptools.find_packages('src/'),
    package_dir={'': 'src'},
    install_requires=requires,
    include_package_data=True,
    namespace_packages=['taba'],
    entry_points={
        'console_scripts': [
            'taba-server = taba.server.launch_taba_server:main',
            'taba-agent = taba.agent.main:main', ], }, )
