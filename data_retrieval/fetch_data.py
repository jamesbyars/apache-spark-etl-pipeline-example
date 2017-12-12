import requests

base_url = "http://data.pystock.com/2016/"

with open("days.txt", 'r') as data_file:
  files = data_file.readlines()


for file in files:
  with open("data/" + file.rstrip(), 'wb') as fd:
    file_name = base_url + file.rstrip()
    print("Fetching: " + file_name)
    r = requests.get(file_name)
    for chunk in r.iter_content(chunk_size=128):
        fd.write(chunk)



