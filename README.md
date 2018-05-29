Helium DataCommons PIVOT
========================
### Architecture

![arch](figures/arch/pivot.png)

### Deployment

The request body for deploying PIVOT on Marathon is as below:

```json
{
  "id": "/sys/pivot",
  "instances": 1,
  "cpus": 2,
  "mem": 2048,
  "disk": 4096,
  "args": [
    "--port",
    "9191",
    "--n_parallel",
    "2"
  ],
  "container": {
    "type": "DOCKER",
    "volumes": [

    ],
    "docker": {
      "image": "heliumdatacommons/pivot",
      "privileged": false,
      "parameters": [

      ],
      "forcePullImage": true
    }
  },
  "networks": [
    {
      "mode": "host"
    }
  ],
  "portDefinitions": [
    {
      "labels": {
        "VIP_0": "/pivot:9191"
      },
      "protocol": "tcp",
      "port": 9191
    }
  ],
  "requirePorts": true,
  "healthChecks": [
    {
      "gracePeriodSeconds": 300,
      "intervalSeconds": 60,
      "maxConsecutiveFailures": 3,
      "portIndex": 0,
      "timeoutSeconds": 20,
      "delaySeconds": 15,
      "protocol": "MESOS_HTTP",
      "path": "/ping"
    }
  ],
  "upgradeStrategy": {
    "minimumHealthCapacity": 0,
    "maximumOverCapacity": 0
  },
  "labels": {
    "DCOS_SERVICE_SCHEME": "http",
    "DCOS_SERVICE_NAME": "pivot",
    "DCOS_PACKAGE_FRAMEWORK_NAME": "pivot",
    "DCOS_SERVICE_PORT_INDEX": "0"
  }
}

```

With curl, the deployment command is as below (assuming the request body
is saved to a file named `pivot.json`):

```shell
curl -X PUT \
    -H "Content-Type: application/json"  \
    -d @pivot.json \
    http://<marathon-host>:<marathon-port>/v2/apps
```

**Note:** The Marathon endpoint is typically protected behind a firewall
and cannot be reached outside the virtual network of DC/OS. Therefore,
to deploy PIVOT, you need to ask the administrator of the DC/OS
cluster to either punch a hole on the firewall for you do the deployment
remotely, or add you as a SSH user to one of the masters to deploy it
locally.

To pin the service onto a specific node, add the `constraints` field in
the request body as below:

```json
{
  ...
  "constraints": [
    [ "hostname", "CLUSTER", "10.52.100.4"]
  ]
  ...
}
```

**Note:** that the hostname is just the private IP address of the DC/OS
agent where PIVOT will land, since DC/OS identifies the agents by their
private IP addresses.
