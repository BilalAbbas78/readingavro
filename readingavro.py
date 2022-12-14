import copy 
import json 
import avro
from avro.datafile import DataFileReader
from avro.io import DatumReader
import pprint
import pandas as pd


path = '/home/ahafeez/Downloads/C78_ALL_AI_GetRadiologyPathology/part-m-00000.avro'

with open(path, 'rb') as f:
    reader = DataFileReader(f, DatumReader())
    metadata = copy.deepcopy(reader.meta)
    schema_from_file = json.loads(metadata['avro.schema'])
    users = [user for user in reader]
    reader.close()


# print(f'Schema from users.avro file:\n {schema_from_file}')
print(len(schema_from_file["fields"]))
pprint.pprint(schema_from_file)


# print(f'Users:\n {users}')
df = pd.DataFrame(users)
print(df.head)