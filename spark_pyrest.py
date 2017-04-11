import os
import pandas as pd 
import requests

os.environ['http_proxy']=""

class SparkPyRest(object): 
    """Basic class for handling Spark REST API queries"""

    def __init__(self, host, port): 
        self.host = host
        self.port = port
        self.base_url = base_url = 'http://{host}:{port}/api/v1'.format(host=host, port=port) 

    @property
    def app(self): 
        return get_app(self.base_url)

    @property
    def stages(self): 
        return get_stages(self.base_url)

    def tasks(self, stageid):
        """Return the tasks for a given stageid"""
        return get_tasks(self.base_url, stageid)


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
    if isinstance(stageid,int): 
        response = requests.get(base_url+'/applications/'+get_app(base_url)+'/stages/'+str(stageid))
        j = response.json()
        res = [(i,t['host'],t['executorId'],t['taskMetrics']['executorRunTime']) for i,t in j[0]['tasks'].iteritems()]
        columns = ['task_id', 'host_ip', 'executor_id', 'task_time']
        res = pd.DataFrame(res,columns=columns)
    elif isinstance(stageid,list):
        columns = ['task_id', 'host_ip', 'executor_id', 'task_time', 'stageid']
        res = pd.DataFrame(columns=columns)
        for s in stageid: 
            df = get_tasks(s)
            df['stageid'] = s
            res = res.append(df)
    return res

