import os
import time
from pprint import pformat, pprint
import pprint
import json

JD = 'test.json'

with open('test.txt', 'w') as f:
    print(pformat(os.listdir()), file=f)

counter = 0

jd = []
if os.path.exists(JD):
    with open(JD, 'r') as f:
        jd = json.load(f)

while True:
    time.sleep(1)
    with open('test.txt', 'a') as f:
        print(f'({counter}, {len(jd)})', file=f, end=', ')
    counter += 1

    jd.append(counter)
    with open(JD, 'w') as f:
        json.dump(jd, f)
