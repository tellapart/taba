#!/bin/bash

PYTHONPATH=../py:../py/tellapart/third_party python ../py/tellapart/taba/agent/taba_agent_main.py $1 >> agent.log 2>&1

