import pymongo
import requests

from tornado.escape import json_encode

from unittest import TestCase


class SchedulerTest(TestCase):

  def setUp(self):
    self.pivot = 'http://localhost:9191'
    self.db = pymongo.MongoClient()

  def test_one_job_app(self):
    app = dict(id='one-job',
               containers=[
                 dict(id='first-job',
                      type='job',
                      image='ubuntu',
                      resources=dict(cpus=1, mem=1024, disk=128),
                      network_mode='container')
               ])
    r = requests.post('%s/appliance'%self.pivot, data=json_encode(app))
    self.assertEqual(201, r.status_code)

  def test_one_parallel_job_app(self):
    app = dict(id='one-job-parallel',
               containers=[
                 dict(id='first-job',
                      type='job',
                      instances=5,
                      image='ubuntu',
                      resources=dict(cpus=1, mem=1024, disk=128),
                      network_mode='container')
               ])
    r = requests.post('%s/appliance' % self.pivot, data=json_encode(app))
    self.assertEqual(201, r.status_code)

  def test_two_jobs_with_dependencies(self):
    app = dict(id='two-job-dep',
               containers=[
                 dict(id='first-job',
                      type='job',
                      instances=5,
                      image='ubuntu',
                      resources=dict(cpus=1, mem=1024, disk=128),
                      network_mode='container',
                      cmd='sleep $(shuf -i 10-20 -n 1)'),
                 dict(id='second-job',
                      type='job',
                      instances=5,
                      image='ubuntu',
                      resources=dict(cpus=.5, mem=512, disk=128),
                      cmd='sleep $(shuf -i 10-20 -n 1)',
                      dependencies=['first-job'])
               ])
    r = requests.post('%s/appliance' % self.pivot, data=json_encode(app))
    self.assertEqual(201, r.status_code)

  def test_node_affinity(self):
    self.db.drop_database('pivot')
    app = dict(id='two-job-dep',
               containers=[
                 dict(id='first-job',
                      type='job',
                      instances=5,
                      image='ubuntu',
                      resources=dict(cpus=1, mem=1024, disk=128),
                      network_mode='container',
                      cmd='sleep $(shuf -i 30-40 -n 1)',
                      schedule_hints=dict(placement=dict(cloud='aws'))),
                 dict(id='second-job',
                      type='job',
                      instances=5,
                      image='ubuntu',
                      resources=dict(cpus=.5, mem=512, disk=128),
                      cmd='sleep $(shuf -i 10-20 -n 1)',
                      dependencies=['first-job'],
                      schedule_hints=dict(placement=dict(cloud='gcp')))
               ])
    r = requests.post('%s/appliance' % self.pivot, data=json_encode(app))
    self.assertEqual(201, r.status_code)

  def test_service_job_mix(self):
    # self.db.drop_database('pivot')
    app = dict(id='mix',
               containers=[
                 dict(id='first-service',
                      type='service',
                      instances=2,
                      image='ubuntu',
                      resources=dict(cpus=1, mem=1024, disk=128),
                      network_mode='container',
                      cmd='tail -f /dev/null'),
                 dict(id='second-job',
                      type='job',
                      instances=5,
                      image='ubuntu',
                      resources=dict(cpus=.5, mem=512, disk=128),
                      cmd='sleep $(shuf -i 10-20 -n 1)',
                      dependencies=['first-service'])
               ])
    r = requests.post('%s/appliance' % self.pivot, data=json_encode(app))
    self.assertEqual(201, r.status_code)

  def test_get_appliance(self):
    r = requests.get('%s/appliance/mix'%self.pivot)
    self.assertEqual(r.status_code, 200)
    import json
    print(json.dumps(r.json(), indent=2))

  def test_delete_appliance(self):
    r = requests.delete('%s/appliance/mix'%self.pivot)
    self.assertEqual(200, r.status_code)