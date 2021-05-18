import json
import os
import csv
import re

path = 'XYZ'
count = 0

with open('airflow_status_slack.csv','w') as data_file:
	csv_writer = csv.writer(data_file)

	for filename in os.listdir(path):
		with open(os.path.join(path, filename)) as f:
			data = f.read()
			obj = json.loads(data)
			#print(filename) --gives date of record
			for o in obj:
				text = o["text"]
				try:
					task = text.split("*Task*:",1)[1].rstrip().split("*Dag*:")[0]
				except IndexError:
					task = "ignore"
				#found = task.rstrip("*Dag*:")[0]
				#found = re.search(r"(?<=*Task*:)...(?=*Dag*:)", text).group(0)
				if count == 0:
					header = "FileName","Task","Text"
					csv_writer.writerow(header)
					count += 1
				csv_writer.writerow([filename,task,text])
				print(csv_writer)
