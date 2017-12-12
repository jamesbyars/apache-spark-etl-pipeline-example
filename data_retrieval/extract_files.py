import os
import tarfile

dir_path = '/Users/james/Development/spark-etl/data'

file_list = os.listdir(dir_path)
data_dest = dir_path + 'extracted'

for archive in file_list:
    file_name = archive.replace('.tar.gz', '') + '.csv'
    with open(archive, 'r:gz') as dest:
        tarfile.open(archive).extract('prices.csv', path=data_dest + '/' + file_name)

