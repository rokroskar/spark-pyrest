import os
import pandas as pd 
import requests
import re

os.environ['http_proxy']=""

class SparkPyRest(object): 
    """Basic class for handling Spark REST API queries"""

    def __init__(self, host, port=4040): 
        self.host = host
        self.port = port
        self.base_url = base_url = 'http://{host}:{port}/api/v1'.format(host=host, port=port) 

    @property
    def app(self): 
        return get_app(self.base_url)


    @property
    def stages(self): 
        return get_stages(self.base_url)


    @property
    def executors(self):
        response = requests.get(self.base_url+'/applications/' + self.app + '/executors')
        executors = response.json()
        return executors


    def tasks(self, stageid):
        """Return the tasks for a given stageid"""
        return get_tasks(self.base_url, stageid)


    def executor_log_bytes(self, executor_id):
        """Find out how big the executor log file is"""
        executors = self.executors

        for executor in executors: 
            if executor['id'] == str(executor_id):
               break

        log_url = executors[executor_id]
        response = requests.get(executor['executorLogs']['stderr'].replace('logPage', 'log')+'&offset=0')
        total_bytes = int(re.findall('\d+\sof\s(\d+)\sof', response.text)[0])
        return total_bytes        


    def executor_log(self, executor_id, nbytes='all'):
        """Fetch the executors log; by default, the entire log is returned."""
        total_bytes = self.executor_log_bytes(executor_id)

        executors = self.executors

        for executor in executors: 
            if executor['id'] == str(executor_id):
               break

        if nbytes=='all':
            nbytes = total_bytes

        else: 
            if not isinstance(nbytes,int): 
                raise RuntimeError('nbytes must be an integer')

        query = executor['executorLogs']['stderr'].replace('logPage', 'log')
        query += '&offset=0'
        query += '&byteLength={}'.format(nbytes)
        response = requests.get(query)

        return response.text


# base public functions

def get_app(base_url):
    """Get the app ID from the REST server"""
    response = requests.get(base_url+'/applications')
    return response.json()[0]['id']


def get_stages(base_url):
    """Get stage IDs"""
    response = requests.get(base_url+'/applications/'+get_app(base_url)+'/stages')
    stages = response.json()
    return [(stage['stageId'], stage['name']) for stage in stages]


def get_tasks(base_url, stageid):
    """Produce a DataFrame of task metrics for a stage or list of stages"""

    fields = ['taskId', 'host', 'executorId', 'executorRunTime', 
                  'localBytesRead', 'remoteBytesRead', 'bytesWritten']

    if isinstance(stageid,int): 
        response = requests.get(base_url+'/applications/'+get_app(base_url)+'/stages/'+str(stageid))
        j = response.json()
        res = [_recurse_dict(task,fields) for task in j[0]['tasks'].values()]
        res = pd.DataFrame(res)

    elif isinstance(stageid,list):
        columns = fields + ['stageid',]
        res = pd.DataFrame(columns=columns)
        for s in stageid: 
            df = get_tasks(base_url, s)
            df['stageid'] = s
            res = res.append(df)
    return res

def _recurse_dict(d, fields):
    """Traverse a dictionary recursively looking for keys matching the list of keys in 'fields'"""
    task_metrics = {}
    for k,v in d.iteritems():
        if k in fields: task_metrics[k] = v
        if isinstance(v,dict):
            new_dict = _recurse_dict(v,fields)
            if new_dict is not None: 
                task_metrics.update(new_dict)
    return task_metrics
