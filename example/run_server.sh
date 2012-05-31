#!/bin/bash

PYTHONPATH=../py:../py/tellapart/third_party python ../py/tellapart/taba/server/launch_taba_server.py $1 >> server.log 2>&1

