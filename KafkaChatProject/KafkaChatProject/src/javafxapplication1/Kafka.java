package javafxapplication1;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class Kafka {
	public KafkaConsumer<String, String> consumer = null;
	public Producer<String, String> producer = null;
    public AdminClient admin =null;
    public ArrayList<TopicFile> files = new ArrayList<TopicFile>();
    public Collection<String> collection = new ArrayList<String>();
    public boolean ForwordFlag = true;
	public boolean ChangeSubscribe = false;
	public boolean status =false;
	public Kafka(String UserName) {
		 this.status = connection(UserName);
	}
    public boolean connection(String UserName) {
		// get Addresses Broker ConfigFile.Properties
		String BrokerAddresses = Config.getConfig("BrokerAddress");
		// init producer
	    Properties prop = hintProducer(BrokerAddresses);
	    this.producer = new KafkaProducer<>(prop);
		if(this.producer == null )
			return false;
		// init Admin
        prop.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 4000);
        try {
		this.admin = AdminClient.create(prop);
        }catch (Exception e) {
			return false;
		}
        if(this.admin == null )
			return false;
        // hint Consumer - 
		prop = hintConsumer(BrokerAddresses,UserName+" "+GetRandomName());
		this.consumer = new KafkaConsumer<>(prop);
		if(this.consumer == null )
			return false;
		this.collection.add("BrodCast");
		this.consumer.subscribe(collection);
		return true;
		//java.util.Collection<Node> nodes;
        //java.util.Collection<PartitionInfo> partitions;
		//Cluster x = new Cluster() ;
		//KafkaServerStartable kafkaServer = new KafkaServerStartable(new KafkaConfig(prop));
	    //kafkaServer.startup();
		//String port = kafkaServer.serverConfig().port().toString();
		//System.out.println("port"+port);
		//this.Sccssed = true;
	}
    public void Close() {
    	this.ForwordFlag = false;
    	this.status = false;
    	this.producer.close();
    	this.admin.close();
    }
    
	private String GetRandomName() {
		byte[] array = new byte[20]; // length is bounded by 7
	    new Random().nextBytes(array);
	    String generatedString = new String(array, Charset.forName("UTF-8"));
	 
	    return generatedString;
	  }
	// init Properties producer
	private Properties hintProducer(String Address) {
	    	if(Address.isEmpty())
				return null;
	    		Properties prop = new Properties();
		       prop.put("bootstrap.servers", Address.toString() );
	           prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	           prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
	           return prop;
	    }
	// init Properties Consumer
	private Properties hintConsumer(String Address,String Name) {
			if(Address.isEmpty())
				return null;
			Properties prop = new Properties();
	        prop.put("bootstrap.servers", Address.toString() );
	        prop.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
	        prop.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
	        prop.put("group.id",Name);
			return prop;     
	  }
	// send Message 
	void SendMessage(String topic,String message) {
		 String key = "key";
		 ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,message);
    	 producer.send(record);
    	 producer.flush();	 
	}
	
	void UnsubscribeTopic(String Topic) {
		collection.remove(Topic);
		ChangeSubscribe =true;
	}
	
	void SubscribeTopic(String Topic) {
		collection.add(Topic);
		ChangeSubscribe =true;
	}
	
	void removeToic(String Topic) {
		
				Map<String, String> configs = new HashMap<>();
				int numPartitions = 1;
				short replicationFactor = 3; 
				// create new Topic
				NewTopic topics = new NewTopic(Topic, numPartitions, replicationFactor);
				Collection<String> c =  Collections.singletonList(topics.configs(configs).toString());
				
				DeleteTopicsResult deleteTopicsResult = admin.deleteTopics(c);
								 
				if(deleteTopicsResult.all().isDone())
		        	ChatFrame.PrintMessage("Delete Topic Successful");
		        else
		        	ChatFrame.PrintMessage("Error Delete Topic");
		
	}
	
	void CreateTopic(String Topic) {
		new Thread(new Runnable() {	
			@Override
			public void run() {
				Map<String, String> configs = new HashMap<>();
				int numPartitions = 1;
				short replicationFactor = 3; 
				
				// create new Topic
				NewTopic topics = new NewTopic(Topic, numPartitions, replicationFactor);
				
		        CreateTopicsResult createTopicsResult = admin.createTopics(
		        				Collections.singletonList(topics.configs(configs)));
		        
		        try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        if(createTopicsResult.all().isDone())  {
		        	ChatFrame.PrintMessage("Create Topic Successful");
		        }else {
		        	ChatFrame.PrintMessage("Error - Create Topic");
		        }
		       /* if(TopicIsExistByZookeeper(Topic)) {
		        	ChatFrame.PrintMessage("Create Topic Successful");
		        }else {
		        	ChatFrame.PrintMessage("Error - Create Topic");
		        }*/
			}
		}).start();
		
	        //consumer.subscribe(Collections.singletonList(Topic));

	}

	void ConsumerForword() {
	  // Forword
		new Thread(new Runnable() {
			
			@Override
		    public void run(){
	            while(ForwordFlag){
	                ConsumerRecords<String, String> records = consumer.poll(100);
	                for(ConsumerRecord<String, String> record: records){
	                	//record.offset(), record.key(), record.value() ,record.timestamp());
	                	ChatFrame.PrintMessage(record.value());	
	                	ChatFrame.PrintMessageToList(record.value());	
	                	String [] line =record.value().split(" ",2);
	                	
	                	TopicFile.println(ChatFrame.UserName,line[0],record.value());
	                }
	                if(ChangeSubscribe) {
                    	consumer.subscribe(collection);
                    	ChangeSubscribe= false;
                	}
	            }
	            consumer.close();
	            ChatFrame.PrintMessage("Forword Closed - Consumer closed");
	    }
		}).start();
	}
	
	boolean TopicIsSubscribe(String Topic) {
		return collection.contains(Topic);
	}
    
	boolean TopicIsExistByZookeeper(String Topic)  {
        
		Set<String> topicSet;
		try {
			topicSet = admin.listTopics().names().get();
			Iterator<String> it = topicSet.iterator();
	        while (it.hasNext()) {
	        	String t = it.next().toString();
	        	if(t.equals(Topic)) {
	                return true;
	        	}
	        }
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
	        return false;
		}
        return false;
	}
}
