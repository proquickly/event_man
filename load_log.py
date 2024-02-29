import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
import pprint
import pandas as pd
import json

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

with open('data/Spark_2k.log','r') as f:
    line_number = 0
    log = []
    for line in f.readlines():
        line = line.strip()
        log.append({'date': line[:8], 'time': line[8:17].strip(), 'level': line[17:22].strip(), 'message': line[22:].strip()})
        line_number += 1

log = pd.DataFrame(log)
#print(log.info())
#print(str(log))

executions = log[log.message.str.contains('executor.Executor: Running')]
print(str(executions['message'].astype('str').split(' ')))