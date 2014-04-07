package edu.sjsu.cmpe.procurement.jobs;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.ws.rs.core.MediaType;


import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;
import edu.sjsu.cmpe.procurement.domain.Book;
import edu.sjsu.cmpe.procurement.domain.ShippedBooks;

/**
 * This job will run at every 5 second.
 */
@Every("45s")
public class ProcurementSchedulerJob extends Job {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private static ArrayList<String>  isbnList=new ArrayList<String>();
   
    final String publisherEndPointUri="http://54.215.133.131:9000/orders";
    final String orderShipmentUri="http://54.215.133.131:9000/orders/38622";
    private Client client;
    
    private  String user = env("APOLLO_USER", "admin");
 	private String password = env("APOLLO_PASSWORD", "password");
 	private String host = env("APOLLO_HOST", "54.215.133.131");
 	private int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
 	private String queue = "/queue/38622.book.orders";
 	private String topic="/topic/38622.book.";
 	
 	private String[] messageToPublish;
    @Override
    public void doJob() {
	String strResponse = ProcurementService.jerseyClient.resource(
		"http://ip.jsontest.com/").get(String.class);
	log.debug("Response from jsontest.com: {}", strResponse);
	try {
		ClientConfig clientConfig = new DefaultClientConfig();
    	clientConfig.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
	    client = Client.create(clientConfig);
		pullMessageFromQueue();
	} catch (JMSException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    }
    


    
    protected void pullMessageFromQueue() throws JMSException{
    	
    	
    	StompJmsConnectionFactory connectionFactory =new StompJmsConnectionFactory();
    	//int port=Integer.parseInt(proConfig.getApolloPort());
    	connectionFactory.setBrokerURI("tcp://" + host + ":" + port);
    
    	Connection connection = connectionFactory.createConnection(user, password);
    	connection.start();
    	
    	//connection.setExceptionListener(this);
    	Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // Create the destination (Topic or Queue)
        Destination dest = new StompJmsDestination(queue);
        
        
       

        // Create a MessageConsumer from the Session to the Topic or Queue
        MessageConsumer consumer = session.createConsumer(dest);
        System.out.println("Waiting for messages from " + ProcurementService.getQueueName() + "...");
        long waitUntil = 5000; // wait for 5 sec
        while(true) {
            Message msg = consumer.receive(waitUntil);
            if( msg instanceof  TextMessage ) {
                   String body = ((TextMessage) msg).getText();
                   parseIsbnFromRequest(body);
                   System.out.println("Received message = " + body);
                  
                  
                   
            }else if (msg instanceof StompJmsMessage) {
        		StompJmsMessage smsg = ((StompJmsMessage) msg);
        		String body = smsg.getFrame().contentAsString();
        		if ("SHUTDOWN".equals(body)) {
        		    break;
        		}
        		System.out.println("Received message = " + body); 
            } if (msg == null) {
                  System.out.println("No new messages. Existing due to timeout - " + waitUntil / 1000 + " sec");
                  break;
            } else {
                 System.out.println("Unexpected message type: " + msg.getClass());
            }
        } // end while loop
        
        if(isbnList.size()>0){
        sendBookRequestsToPublisher();
        }
        ShippedBooks response=receiveShippedBooksFromPublisher();
        generateMessageForPublishing(response);
        System.out.println(response);
        
        Connection conn=createStompConnection();
        startPublishing(conn);
        
        
        connection.close();
        System.out.println("Done");

    	
    }
    
    private void  generateMessageForPublishing(ShippedBooks shippedBooks){
    	String msg=null;
    	
    	List<Book> books=shippedBooks.getShipped_books();
    	int numOfBooks=books.size();
    	messageToPublish=new String[numOfBooks];
    	for(int i=0;i<numOfBooks;i++){
    		Book book=(Book)books.get(i);
    	 msg=book.getIsbn()+":"+book.getTitle()+":"+book.getCategory()+":"+book.getCoverimage();
    	 System.out.println("message generated "+msg);
    	 messageToPublish[i]=msg;
    	}
    	
    	
    }
    
    private void  startPublishing(Connection connection)throws JMSException{
    	
         String destTopic=null;    
    	 Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         // Create the destination (Topic or Queue)
    	 for (int i=0; i<messageToPublish.length;i++){
    		 String[] tokens=messageToPublish[i].split(":");
    		 if(tokens[2].equals("computer"))
    			 destTopic=topic+"computer"; 
    		 if(tokens[2].equals("comics"))
    			 destTopic=topic+"comics";
    		 if(tokens[2].equals("management"))
    			 destTopic=topic+"management";
    		 if(tokens[2].equals("selfimprovement"))
    			 destTopic=topic+"selfimprovement";
    		  Destination dest = new StompJmsDestination(destTopic);
        	  MessageProducer producer = session.createProducer(dest);
        	  producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        	  TextMessage message = session.createTextMessage(messageToPublish[i]);
        	  producer.send(message);
              
    	 }
        
    }
         
       
         
         
     
    
    
    private void sendBookRequestsToPublisher(){
    	
    	HashMap<String, Serializable> request = new HashMap<String, Serializable>();
	    request.put("id", "38622");
//	   
	    request.put("order_book_isbns", isbnList);
	    System.out.println(request);
    	
	    
	    WebResource resource = client.resource( publisherEndPointUri );
	   
    	ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
    	        .type( MediaType.APPLICATION_JSON )
    	        .entity(request)
    	        .post( ClientResponse.class );
    	System.out.println("response recieved from publisher"+response.getStatus());
    	
    	
    	
		
    }
    
    private ShippedBooks receiveShippedBooksFromPublisher() {
		// TODO Auto-generated method stub
    	 WebResource order = client.resource(orderShipmentUri );
    	ShippedBooks books = order.accept("application/json" )
    	        .type("application/json" )
    	        .get( ShippedBooks.class );
    	System.out.println("response recieved from publisher"+books);
    	return books;
	}




	private void parseIsbnFromRequest(String msg){
    	String delim=":";
    	   
    		String[] tokens=msg.split(delim);
    		isbnList.add(tokens[1]);
    	
    	System.out.println("isbn list"+isbnList);
    }
	
	private Connection  createStompConnection()throws JMSException{
		
		
		StompJmsConnectionFactory connectionFactory =new StompJmsConnectionFactory();
        connectionFactory.setBrokerURI("tcp://" + host + ":" + port);
    
    	Connection connection = connectionFactory.createConnection(user, password);
        connection.start();
        return connection;
       
	}
    
    private static String env(String key, String defaultValue) {
    	 	String rc = System.getenv(key);
    	 	if( rc== null ) {
    	 	    return defaultValue;
    	 	}
    	 	return rc;
    	    }
    	 
       
    
}

