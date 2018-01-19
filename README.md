# KafkaProject
Chat By Topic - with Kafka Server Borkers - ZooKeeper - Kinneret College
-----------------------------------------------------------------------

Student group: Ahmad igbarea/Loay naser

The project take approximately 20 hours of working in group.

Run instruction:
To run the KafkaServer you need to start it via the shell command line,  

Kafka uses ZooKeeper so you need to first start a ZooKeeper server if you don't already have one. 
You can use the convenience script packaged with kafka to get a quick-and-dirty single-node ZooKeeper instance. 
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
-----------------------------------------------------------------------
Now start the Kafka server:
bin\windows\kafka-server-start.bat config\server.properties
-----------------------------------------------------------------------

Type in the following instruction: java –jar kafkaProject.jar  
There is other option to run via the Main.java from the source code.

the config.proporties file will show the port number in the following format:-

---------
#fileConfig
BrokerAddress = localhost:9092 ,localhost:9093 ,localhost:9094

-----------------------------------------------------------------------

