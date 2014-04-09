package edu.sjsu.cmpe.library;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.assets.AssetsBundle;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.views.ViewBundle;

import edu.sjsu.cmpe.library.api.resources.BookResource;
import edu.sjsu.cmpe.library.api.resources.RootResource;
import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;
import edu.sjsu.cmpe.library.repository.BookRepository;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;
import edu.sjsu.cmpe.library.ui.resources.HomeResource;

public class LibraryService extends Service<LibraryServiceConfiguration> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) throws Exception {
	new LibraryService().run(args);
	
    }

    @Override
    public void initialize(Bootstrap<LibraryServiceConfiguration> bootstrap) {
	bootstrap.setName("library-service");
	bootstrap.addBundle(new ViewBundle());
	bootstrap.addBundle(new AssetsBundle());
    }
    
    protected ConnectionFactory  createConnection(String host ,String port){
    	StompJmsConnectionFactory result = new StompJmsConnectionFactory();
    	result.setBrokerURI("tcp://" +host+":"+port);
    	return result;
    	
    }

    @Override
    public void run(LibraryServiceConfiguration configuration,
	    Environment environment) throws Exception {
	// This is how you pull the configurations from library_x_config.yml
	String queueName = configuration.getStompQueueName();
	 String topicName = configuration.getStompTopicName();
	String libraryName=configuration.getLibraryName();
	log.debug("{} - Queue name is {}. Topic name is {}",
		configuration.getLibraryName(), queueName,
		topicName);
	log.info("{} - Queue name is {}. Topic name is {}",
		configuration.getLibraryName(), queueName,
		topicName);
	// TODO: Apollo STOMP Broker URL and login
	
	String user=configuration.getApolloUser();
	String password=configuration.getApolloPassword();
	String host=configuration.getApolloHost();
	String port=configuration.getApolloPort();
	ConnectionFactory connectionFactory=createConnection(host,port);
	Connection connection = connectionFactory.createConnection(user, password);
    connection.start();
	
	log.debug("{} - Apollo user name  is {}",
			libraryName, user);
	log.debug("{} - Apollo password   is {}",
			configuration.getLibraryName(), password);
	
	log.debug("{} - Apollo host name   is {}",
			configuration.getLibraryName(), host);
	log.debug("{} - Apollo port   is {}",
			configuration.getLibraryName(), port);
	/** Root API */
	environment.addResource(RootResource.class);
	/** Books APIs */
	 BookRepositoryInterface bookRepository = new BookRepository();
	environment.addResource(new BookResource(bookRepository,connection,queueName,libraryName,topicName));
	
	
	/** UI Resources */
	environment.addResource(new HomeResource(bookRepository));
	
   int numOfThreads = 2;
	ExecutorService execute = Executors.newFixedThreadPool(numOfThreads);
	
	
	Runnable listener=new Listener(bookRepository, topicName, connection); 
	execute.execute(listener);
	
	
    }
}
