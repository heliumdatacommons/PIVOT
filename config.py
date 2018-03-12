import json

config = json.load(open('config.json'))

cluster_to_ip = dict(
  azure='13.90.204.206',
)

ip_to_cluster = {
  '13.90.204.206': 'azure',
}
