## Spark
sudo yum update -y
sudo yum install -y wget java-1.8.0-openjdk* epel-release
wget http://www.scala-lang.org/files/archive/scala-2.10.1.tgz
tar xvf scala-2.10.1.tgz
sudo mv scala-2.10.1 /usr/lib
sudo ln -s /usr/lib/scala-2.10.1 /usr/lib/scala
export PATH=$PATH:/usr/lib/scala/bin

sleep 5

## Docker
sudo yum install -y yum-utils device-mapper-persistent-data lvm2
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install -y docker-ce
sudo systemctl start docker

sudo docker run --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
sleep 10
sudo docker exec -i --user postgres some-postgres bash -c "psql -c 'CREATE TABLE stock_data (symbol text,date date,open int,high int,low int,close int,volume int,adj_close int,month int,year int,day int);'"
sudo docker exec -i --user postgres some-postgres bash -c "psql -c 'CREATE TABLE avg_month_close (month int,average_month_close int);'"
sudo docker exec -i --user postgres some-postgres bash -c "psql -c 'CREATE TABLE adjusted_close_count (month int,count int);'"
