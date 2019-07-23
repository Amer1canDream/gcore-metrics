import requests
import json
import graphyte
from datetime import timedelta, datetime
import os
import time

graphite_host = os.environ['GRAPHITE_HOST']
graphite_port = os.environ['GRAPHITE_PORT']
resources = eval(os.environ['RESOURCES'])
username = os.environ['USERNAME']
password = os.environ['PASSWORD']
section = os.environ['SECTION']
metric_prefix = os.environ['METRIC_PREFIX']
metrics = eval(os.environ['METRICS'])

def getToken():
    payload = {'username': username, 'password': password}
    headers = {'Content-type': 'application/json'}
    request = requests.post("https://api.gcdn.co/auth/signin", data=json.dumps(payload), headers=headers)
    response = json.loads(request.text)
    token = response['token']
    return(token)

def getStat(section, metrics):
    date_time = datetime.utcnow()
    now = str(date_time).replace(' ', 'T')[:-10]
    beforeAnyTime = str(date_time - timedelta(seconds=int(section))).replace(' ', 'T')[:-10]
    token = getToken()
    for metric in metrics:
        headers = {'Authorization': 'Token ' + token, 'Content-Type': 'application/json'}
        payload = {'service': 'CDN', 'from': beforeAnyTime, 'to': now, 'group_by': 'resource', 'granularity': '1m',
                   'metrics': metric}
        request = requests.get("https://api.gcdn.co/statistics/series?query_string", headers=headers, params=payload)
        graphyte.init(graphite_host, port=graphite_port, prefix=metric_prefix)
        try:
            response = json.loads(request.text)
            if response == {}:
                pass
            else:
                totalValues = response['resource']
                for i in resources.keys():
                    try:
                        metric = list(totalValues[i]['metrics'].keys())[0]
                        values = totalValues[i]['metrics'][metric]
                        prefix = i + '_' + resources[i].replace('.', '_') + '.' + metric
                        #print(prefix)
                        for v in values:
                             graphyte.send(prefix, v[1], v[0])
                             #print(prefix, v[1], v[0])
                    except:
                        pass
        except:
            pass

while True:
    try:
        time.sleep(60)
        getStat(section, metrics)
    except:
        pass
