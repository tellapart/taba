Overview
====================

A Taba deployment consists of 6 layers, each horizontally scalable. These
layers are:

- Taba Client code integrated into applications
- Taba Agent process running locally to the application servers
- Taba Server processes in the frontend ('fe') role
- Taba Server processes in the backend ('be') role
- Redis sentinel processes
- Redis database processes

The Taba Client is integrated into the application it is instrumenting, and
exposes an API for recording events to different counter types. The Python
distribution includes a default Client implementation based on threads, and
a Gevent engine. There is also a Java Client available
([https://github.com/tellapart/taba-java-client]
(https://github.com/tellapart/taba-java-client))

The Client typically sends events to a Taba Agent process running on the same
server. While the Client will only forward events on a best-effort basis, the
Agent provides more robust buffering and failure recovery. It is also
significantly smarter about load balancing.

The will forward events to one of the Taba Server end-points it has been
configured to connect to. Any Server in the cluster can receive any set of
events.

The Taba Server processes are split into two groups: frontend ('fe') and backend
('be'). These roles are configured when the process starts. Assigning a process
a 'fe' role has no effect on its operation -- it is simply a marker that the
process will use to advertise its role. (The intention it to allow a
load-balancer to use that indicator to route traffic to just the 'fe'
processes). Assigning the 'be' role will configure the process to launch a
background worker that processes queued events. There must be at least one 'be'
Server process in the cluster.

A Server process can be assigned both 'fe' and 'be' roles. For small clusters,
this will work well. However, once a cluster becomes large enough to require
multiple Server processes, separating 'fe' and 'be' processes will perform
better.

There is a third role 'ctrl', which essentially marks a Server process as
neither 'fe' nor 'be'. This is useful for maintaining a separate set of
processes for querying.

The Taba Server uses a group of Redis databases and Sentinels. Having at least
one Sentinel is a requirement, as it is used for service discovery of the
individual database processes. Sharding across the databases is accomplished by
splitting the key-space into a large number of virtual buckets, and assigning
ranges of buckets to each process.

Example
====================

This example will demonstrate how to setup a basic cluster on a single server.

Setting up Redis
--------------------

First, we will need a Redis database. Download and compile Redis using the
instructions on the [Redis Downloads page](http://redis.io/download).

We will be using a 2 shard cluster. First, start the database processes:

    $ src/redis-server --port 17000 > redis_17000.log 2>&1 &
    $ src/redis-server --port 17001 > redis_17001.log 2>&1 &

Next we need a basic Sentinel configuration file. For example, in sentinel.conf:

    port 18000
    sentinel monitor shard1 localhost 17000 1
    sentinel monitor shard2 localhost 17001 1

Finally, start the Sentinel process:

    $ src/redis-server sentinel.conf --sentinel > sentinel.log 2>&1 &

Taba Server
--------------------

To install all the Taba components, we can use PyPI:

    pip install taba

Or install directly from the repository:

    git clone https://github.com/tellapart/taba.git
    cd taba
    python setup.py install

We will be using a single Taba Server process configured as both 'fe' and 'be'.
The configuration file for the Server and then Agent are in the docs/ directory.

    $ taba-server taba_server_settings.json &
    $ taba-agent taba_agent_settings.json &

Using the Client
--------------------

There is an example client in the /docs folder. Run it to record some events to
the service.

    $ python example_client.py

Now we can check that the events were recorded.

    $ curl localhost:8300/clients
    example_client
    taba_server.127.0.0.1

    $ curl localhost:8300/names
    example_gauge
    example_moving_counter
    taba_events_recieved
    taba_latency_enqueue_sec
    taba_latency_task_in_queue_ms
    taba_latency_task_processed_ms
    taba_post_zip_time_ms
    task_process_time_time

    $ curl localhost:8300/tabs?name=example_gauge
    example_gauge: {"value": 1}

    $ curl localhost:8300/tabs?name=example_moving_counter
    example_moving_counter: {
      "1m": {"count": 2, "total": 1100.00, "average": 550.00},
      "10m": {"count": 2, "total": 1100.00, "average": 550.00},
      "1h": {"count": 2, "total": 1100.00, "average": 550.00},
      "1d": {"count": 2, "total": 1100.00, "average": 550.00},
      "pct": {"count": 2, "total": 1100.000000,
        "pct": [100.00, 1000.00, 1000.00, 1000.00, 1000.00, 1000.00, 1000.00]}}
