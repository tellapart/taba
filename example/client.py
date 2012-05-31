from gevent import monkey
monkey.patch_all()

import gevent
from tellapart.taba import taba_client

taba_client.Initialize('test_client', 'http://localhost:8280/post')
taba_client.RecordValue('test_name', (100, ))
taba_client.RecordValue('test_name', (1000, ))

gevent.sleep(1)
taba_client.Flush()

