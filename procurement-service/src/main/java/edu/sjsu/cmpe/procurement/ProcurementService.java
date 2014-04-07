package edu.sjsu.cmpe.procurement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.client.JerseyClientBuilder;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

import de.spinscale.dropwizard.jobs.JobsBundle;
import edu.sjsu.cmpe.procurement.api.resources.RootResource;
import edu.sjsu.cmpe.procurement.config.ProcurementServiceConfiguration;

public class ProcurementService extends Service<ProcurementServiceConfiguration> {

    private final Logger log = LoggerFactory.getLogger(getClass());
  
	private static String queueName;
    private static String topicName;
    private static String user;
    private static String password;
    private static String host;
    private static String port;

    /**
     * FIXME: THIS IS A HACK!
     */
    public static Client jerseyClient;
    
    
    public static String getQueueName() {
  		return queueName;
  	}

  	public static void setQueueName(String queueName) {
  		ProcurementService.queueName = queueName;
  	}

  	public static String getTopicName() {
  		return topicName;
  	}

  	public static void setTopicName(String topicName) {
  		ProcurementService.topicName = topicName;
  	}

  	public static String getUser() {
  		return user;
  	}

  	public static void setUser(String user) {
  		ProcurementService.user = user;
  	}

  	public static String getPassword() {
  		return password;
  	}

  	public static void setPassword(String password) {
  		ProcurementService.password = password;
  	}

  	public static String getHost() {
  		return host;
  	}

  	public static void setHost(String host) {
  		ProcurementService.host = host;
  	}

  	public static String getPort() {
  		return port;
  	}

  	public static void setPort(String port) {
  		ProcurementService.port = port;
  	}


    public static void main(String[] args) throws Exception {
	new ProcurementService().run(args);
    }

    @Override
    public void initialize(Bootstrap<ProcurementServiceConfiguration> bootstrap) {
	bootstrap.setName("procurement-service");
	/**
	 * NOTE: All jobs must be placed under edu.sjsu.cmpe.procurement.jobs
	 * package
	 */
	bootstrap.addBundle(new JobsBundle("edu.sjsu.cmpe.procurement.jobs"));
    }

    @Override
    public void run(ProcurementServiceConfiguration configuration,
	    Environment environment) throws Exception {
	jerseyClient = new JerseyClientBuilder()
	.using(configuration.getJerseyClientConfiguration())
	.using(environment).build();

	/**
	 * Root API - Without RootResource, Dropwizard will throw this
	 * exception:
	 * 
	 * ERROR [2013-10-31 23:01:24,489]
	 * com.sun.jersey.server.impl.application.RootResourceUriRules: The
	 * ResourceConfig instance does not contain any root resource classes.
	 */
	environment.addResource(RootResource.class);

	 queueName = configuration.getStompQueueName();
	 topicName = configuration.getStompTopicPrefix();
	log.debug("Queue name is {}. Topic is {}", queueName, topicName);
	// TODO: Apollo STOMP Broker URL and login
  
	 user=configuration.getApolloUser();
	 password=configuration.getApolloPassword();
	 host=configuration.getApolloHost();
	 port=configuration.getApolloPort();
	log.debug("{} - Apollo user name  is {}",
			configuration.getStompQueueName(), user);
	log.debug("{} - Apollo password   is {}",
			configuration.getStompQueueName(), password);
	
	log.debug("{} - Apollo host name   is {}",
			configuration.getStompQueueName(), host);
	log.debug("{} - Apollo port   is {}",
			configuration.getStompQueueName(), port);
    }
}
