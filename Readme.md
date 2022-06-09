# Docker project
Wikipedia project

## Team: [Liia_Dulher](https://github.com/LiiaDulher)

### API 
todo write

### Prerequiments
````
$ pip install --upgrade pip
$ pip install pytz
$ pip install sseclient
$ pip install cassandra-driver
````
If you want to use given client for communication:
````
$ pip install requests
````

### Important
It takes about 65 seconds for Cassandra node to start, so <i>run-cassandra.sh</i> will start node about <b>1 minute</b>.

### Usage
````
$ sudo chmod +x run-cassandra-cluster.sh
$ sudo chmod +x shutdown-cassandra-cluster.sh
$ sudo chmod +x cassandra-app.sh
$ sudo chmod +x shutdown-app.sh
````
````
$ ./run-cassandra-cluster.sh
$ ./cassandra-app.sh
$ python3 read-from-stream-write-to-cassandra.py
````
````
$ ./shutdown-app.sh
$ ./shutdown-cassandra-cluster.sh
````
Use given client or any other way to send GET requests to localhost:8080.
### Results
todo write
