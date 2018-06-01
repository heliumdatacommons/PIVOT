Running CWL Workflow Launcher
=============================
Running a CWL workflow calls for three components - a TOIL instance that
orchestrates the workflow jobs, a Chronos framework for running the
workflow jobs and a Zookeeper instance for keeping Chronos states.
Hence, a CWL workflow appliance consists of three containers, each of
which corresponds to a component for running the workflow as shown
below:

```json
{
  "id": "toil",
  "containers": [
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
      "id": "chronos",
      "type": "service",
      "resources": {
        "cpus": 1,
        "mem": 1024
      },
      "network_mode": "container",
      "image": "heliumdatacommons/chronos",
      "is_privileged": true,
      "ports": [
        {
          "container_port": 8080,
          "host_port": 0,
          "protocol": "tcp"
        }
      ],
      "args": [
        "--master", "zk://zk-1.zk:2181,zk-2.zk:2181,zk-3.zk:2181,zk-4.zk:2181,zk-5.zk:2181/mesos",
        "--zk_hosts", "@zookeeper:2181"
      ],
      "dependencies": [
        "zookeeper"
      ]
    },
    {
      "id": "toil-launcher",
      "type": "service",
      "resources": {
        "cpus": 1,
        "mem": 1024
      },
      "network_mode": "container",
      "image": "heliumdatacommons/datacommons-base",
      "is_privileged": true,
      "ports": [
        {
          "container_port": 22,
          "host_port": 0,
          "protocol": "tcp"
        }
      ],
      "env": {
        "CHRONOS_URL": "http://@chronos:8080",
        "IRODS_HOST": "test.commonsshare.org",
        "IRODS_PORT": "1247",
        "IRODS_HOME": "/commonssharetestZone/home/kferriter",
        "IRODS_USER_NAME": "kferriter",
        "IRODS_PASSWORD": "kferriter890",
        "IRODS_ZONE_NAME": "commonssharetestZone",
        "SSH_PUBKEY": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC303e2y8aUaMQ1IkHWnGFyb5XykxOM5pLK83XFxWZMKsbYcgmkoODZ4w4COratlQPyMXSz7yaFUbYUccXjIjz8SDZf/9c3xI0UuILOiVfb5Ql/16cimtK65ogns1mHzACCpXZ+mKJSDlGcLFfTGixsF8RaF0tZDY/NIxl8P+3EdY0svBbBaI3fc4cOYL5/Q5S8QvSacxXBAPHEzq7RD2Bq0WDxAhiH4XSXfe/xk+TORZYK3CE3Oqu9p77nrFM7W3M5khsb5Qg/z0W1TQmVWvo5/i3QbDK6YaWhw/0DXjfCeEtdlTVdIq1EJxMWuJnm5IptB1EtG9GBhuHq5Ct2XkUh "
      },
      "args": [ "sshd" ],
      "dependencies": [ "chronos" ]
    }
  ]
}
```

In detail, the `zookeeper` container uses the
`heliumdatacommons/zookeeper` image, which listens on port `2181` in the
container, which will be physically mapped to a ephemeral port on the
host where the container is running. The Zookeeper instance is dedicated
to keeping states for the Chronos in this appliance.

The `chronos` container uses the `heliumdatacommons/chronos` image,
which listens on port `8080` in the container. Since the chronos talks
to the Mesos cluster underneath in the DC/OS cluster, it takes the URL
of the Mesos master using the `--master` option. In the meantime, it
stores the state in the dedicated Zookeeper, it takes the *short ID* of
the Zookeeper container, `@chronos`, using the `--zk_hosts` option. The
*short ID* will be translated into an internal fully-qualified domain
name (FQDN) by PIVOT after submission.

The `toil-launcher` container is the endpoint for user to SSH onto. It
requires a Chronos endpoint to submit workflow jobs to, which can be set
with the environment variable `CHRONOS_URL`. In addition, to enable SSH
login, the container also reads the SSH public key from the
environment variable `SSH_PUBKEY` passed in by users, which will be
populated on-the-fly once the container gets up and running.

To run the appliance, submit the request to PIVOT using a `POST` method
as below:

```shell
curl -X POST -d @appliance.json http://<pivot-url>:<pivot-port>/appliance
```

To get the state of the appliance, query against PIVOT using a `GET`
method as below:

```shell
curl -X GET http://<pivot-url>:<pivot-port>/appliance/toil
```

The response will present the endpoint(s) of each container in the
appliance as below:

```json
{
    "id": "toil",
    "containers": [
        {
            "id": "zookeeper",
            "endpoints": [
                {
                    "host": "54.227.40.165",
                    "host_port": 30904,
                    "container_port": 2181,
                    "protocol": "tcp"
                }
            ],
            "state": "running",
            ...
        },
        {
            "id": "chronos",
            "endpoints": [
                {
                    "host": "18.220.249.88",
                    "host_port": 20482,
                    "container_port": 8080,
                    "protocol": "tcp"
                }
            ],
            "state": "running",
            ...
        },
        {
            "id": "toil-launcher",
            "endpoints": [
                {
                    "host": "13.58.175.56",
                    "host_port": 23302,
                    "container_port": 22,
                    "protocol": "tcp"
                }
            ],
            "state": "running",
            ...
        }
    ]
}
```

To delete the state of the appliance:

```
curl -X DELETE http://<pivot-url>:<pivot-port>/appliance/toil
```
