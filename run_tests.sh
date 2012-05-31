#!/bin/bash
#
# Copyright (C) 2012 TellApart, Inc. All Rights Reserved.
#

pushd pytest
PYTHONPATH=../py:../py/tellapart/third_party:. python tellapart/all_tests.py
popd

