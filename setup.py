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
    'cjson >= 1.0.5',
    'cython >= 0.13',
    'gevent == 0.13.1b', ]

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Environment :: No Input/Output (Daemon)',
    'Natural Language :: English',
    'Operating System :: POSIX', ]

dist = setuptools.setup(
    name='taba',
    version='0.3.1',
    license='Apache License, Version 2.0',
    url='http://github.com/tellapart/taba',
    classifiers=CLASSIFIERS,
    author='Kevin Ballard',
    author_email='kevin@tellapart.com',
    packages=setuptools.find_packages(),
    install_requires=requires,
    include_package_data=True,
    namespace_packages=['taba'],
    entry_points={
        'console_scripts': [
            'taba-server = taba.server.launch_taba_server:main',
            'taba-agent = taba.agent.main:main',
        ],
    },
)