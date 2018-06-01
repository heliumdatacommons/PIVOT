Running Containerized Mesos Cluster
===================================
In the tutorial on [Run CWL Workflow Launcher on PIVOT](./cwl.md), the
Chronos in the appliance directly interacts with the Mesos in the DC/OS
cluster underneath that provisions the resources. So in theory the
workflow can exhaust the resources, since the appliance has access to
all the resources on the cluster and the amount of resources it can
consume is unbounded (although it can be audited and limited by
[quota](http://mesos.apache.org/documentation/latest/quota/)). This may
not be ideal from the perspectives of both the service provider and the
users - the service provider would try to service the users in a fair
manner and prevent malicious contention for resources, while the users
would avoid oversubscribing the resources and running over the budget.
Therefore, we introduce the containerized Mesos cluster, which can be a
part of an appliance, to allow users to reserve resources in advance and
run Mesos applications without interacting with the Mesos running on the
physical cluster.

The containerized Mesos cluster has a Zookeeper instance, a master and
a number of agents just as a regular Mesos cluster on physical machines.
The JSON snippet for the Mesos cluster is as below:

```json
{
    ...
    {
        "id": "zookeeper",
        "type": "service",
        "resources": {
            "cpus": 1,
            "mem": 1024,
            "disk": 4096
        },
        "network_mode": "container",
        "image": "heliumdatacommons/zookeeper",
        "is_privileged": true,
        "ports": [
            {
                "container_port": 2181,
                "host_port": 0,
                "protocol": "tcp"
            }
        ]
    },
    {
        "id": "mesos-master",
        "type": "service",
        "resources": {
            "cpus": 1,
            "mem": 1024
        },
        "network_mode": "container",
        "image": "heliumdatacommons/mesos-master",
        "is_privileged": true,
        "ports": [
            {
                "container_port": 5050,
                "host_port": 0,
                "protocol": "tcp"
            }
        ],
        "args": [
            "--ip=0.0.0.0",
            "--zk=zk://@zookeeper:2181/mesos",
            "--quorum=1",
            "--work_dir=/var/lib/mesos"
        ],
        "dependencies": [ "zookeeper" ]

    },
    {
        "id": "mesos-agent",
        "type": "service",
        "resources": {
            "cpus": 4,
            "mem": 4096
        },
        "network_mode": "container",
        "image": "heliumdatacommons/mesos-agent",
        "is_privileged": true,
        "args": [
            "--master=zk://@zookeeper:2181/mesos",
            "--work_dir=/var/lib/mesos"
        ],
        "volumes": [
            {
                "container_path": "/var/run/docker.sock",
                "host_path": "/var/run/docker.sock",
                "mode": "RW"
            }
        ],
        "dependencies": [ "mesos-master" ]
    }
    ...
}
```

The Mesos master uses the `heliumdatacommons/mesos-master` image, which
takes all the Mesos master configurations documented
[here](http://mesos.apache.org/documentation/latest/configuration/master/).
Similarly, the Mesos agent uses the `heliumdatacommons/mesos-agent`
image, which takes all the Mesos agent configurations documented
[here](http://mesos.apache.org/documentation/latest/configuration/agent/).
As introduced in other tutorials, the containers can reference to each
other directly using their IDs prefixed with the symbol `@` in `cmd`,
`args` and `env`, which will be converted into internal fully-qualified
domain names (FQDNs) by PIVOT for communications between containers
across physical nodes in the cluster.

Note that the Mesos agent mounts the docker socket on its host to the
container. It is needed since Docker does not support nested containers
out-of-the-box. By mounting the socket, the containerized Mesos agent
shares the same Docker stack with its host and is able to logically run
nested containers, although all the containers it creates are
administrated by its host's Docker stack.

*[Docker-In-Docker (DinD)](https://github.com/jpetazzo/dind)* can be a
way to decouple the containerized Mesos agent from its host Docker
stack. However, this solution is experimental and needs further
investigation to identify its potential limit.



