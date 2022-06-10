# Docker project
Wikipedia project

## Team: [Liia_Dulher](https://github.com/LiiaDulher)

### Documentation 
All coresponding documentation: design, API, db diagrams are in <b>documents</b> folder.

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
As time I used <b>UTC +0</b>, because it is time Wikipedia gives in its data. Please take it into considaration, when looking at time in requests and reponses.<br>
My client will tell you UTC time, when runned.
It takes about 65 seconds for Cassandra node to start, so <i>run-cassandra-cluster.sh</i> will start node about <b>1 minute</b>.

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
````
$ python3 client.py
````

### Results
Directory example_queries contains queries and responses for them. It was created at 2022-06-10 15:58:36.263250+00:00.  
