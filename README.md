# [Zatt](https://github.com/g60726/zatt)

Zatt is a distributed storage system built on the [Raft](https://raft.github.io/)
consensus algorithm.

By default, clients share a `dict` data structure

![Zatt Logo](docs/logo.png?raw=true "Zatt Logo")

Please note that the **client** is compatible with both `python2` and `python3`,
while the **server** makes heavy use of the asynchronous programming library
`asyncio` and is therefore python3-only. This won't affect compatibility with
legacy code since the server is standalone.

## Structure of the project

The most relevant part of the code concerning Raft is in the zatt/server/states.py file

## Installing

Please follow the instructions below to install Zatt:

### Cloning
```
$ git clone https://github.com/g60726/zatt.git
$ cd zatt
$ python3 setup.py install
```

### Installing Dependencies
The dependencies are all specified in the requirements.txt file. If you are using virtualenv, then you can use:

```
$ virtualenv -p python3 .env         # Creates a virtual environment with python3
$ source .env/bin/activate           # Activate the virtual environment
$ pip install -r requirements.txt    # Install all the dependencies
```

At this point, you should be able to run:
`$ zattd -h `

## Launching a Cluster

### Spinning up a cluster of servers

A server can be configured with a config file. The project should come with a 
working zatt.conf already. The provided config file allows up to 4 servers 
nodes and 3 clients.

You can now run the first node:

`$ zattd -c zatt.conf -i 0 --type server`

This tells zattd to run the node with `id:0`, taking the info about address,
port, public/private keys, etc from the config file.

Now you can spin up three more nodes: open more terminals and issue:

`$ zattd -c zatt.conf -i 1 --type server`
`$ zattd -c zatt.conf -i 2 --type server`
`$ zattd -c zatt.conf -i 3 --type server`


### Client

To interact with the cluster, we need to spin up a client instance and connect
to it. To spin up two client instances:

`$ zattd -c zatt.conf --type client`

To connect to the client instance, open a python interpreter (`$ python`) and 
run the following commands:

```
In [1]: from zatt.client import DistributedDict
In [2]: d = DistributedDict('127.0.0.1', 9116)
In [3]: d['key1'] = 0
```

Let's retrieve `key1` from a second client:

Open the python interpreter on another terminal and run:

```
In [1]: from zatt.client import DistributedDict
In [2]: d = DistributedDict('127.0.0.1', 9117)
In [3]: d['key1']
Out[3]: 0
```

### Launching a chaos server

We also allow nodes to be launched in Byzantine mode. This means that they 
randomly spew syntactically correct, properly signed, but essentially random
messages onto the cluster network. To do this, kill one of your server instances
from before and run:

`$ zattd -c zatt.conf -i <server-id> --type chaos`

### Notes

Please note that in order to erase the log of a node, the corresponding `zatt.{id}.persist` folder has to be removed.

## Tests
In order to run the tests:

* navigate to the test folder: `cd zatt/tests`
* execute: `python3 run.py`

