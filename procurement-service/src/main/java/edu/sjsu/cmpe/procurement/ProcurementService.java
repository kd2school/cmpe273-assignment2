package edu.sjsu.cmpe.procurement;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import de.spinscale.dropwizard.jobs.JobsBundle;

import edu.sjsu.cmpe.procurement.config.ProcurementServiceConfiguration;

public class ProcurementService extends Service<ProcurementServiceConfiguration> {

	private final Logger log = LoggerFactory.getLogger(getClass());
	public static String queueName;
	public static String topicName;
	public static WebResource postPublisher;
	public static WebResource getPublisher;
	
	

	public static void main(String[] args) throws Exception {
		new ProcurementService().run(args);
		
	}

	@Override
	public void initialize(Bootstrap<ProcurementServiceConfiguration> bootstrap) {
		bootstrap.setName("procurement-service");
		bootstrap.addBundle(new JobsBundle("edu.sjsu.cmpe.procurement.jobs"));
	}


//	public Connection connect () throws JMSException
//	{
//		String user = env("APOLLO_USER", "admin");
//		String password = env("APOLLO_PASSWORD", "password");
//		String host = env("APOLLO_HOST", "54.215.210.214");
//		int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
//		
//		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
//		factory.setBrokerURI("tcp://" + host + ":" + port);
//		
//		Connection connection = factory.createConnection(user, password);
//		connection.start();
//		
//		return connection;
//	}


	public Client createClient()
	{
		ClientConfig cc = new DefaultClientConfig();
		cc.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);

		return Client.create(cc);
	}
	
	
	@Override
	public void run(ProcurementServiceConfiguration configuration,
			Environment environment) throws Exception 
	{
		this.queueName = configuration.getStompQueueName();
		this.topicName = configuration.getStompTopicName();

		String postPub = configuration.getPostPub();
		String getPub = configuration.getGetPub();

		log.debug("Queue name is {}. Topic A is {}.", queueName, topicName);
		log.debug("Post URI {}. Get URI {}", postPub, getPub );


		this.postPublisher = createClient().resource(configuration.getPostPub());		
		this.getPublisher = createClient().resource(configuration.getGetPub());
		


	}



	private static String arg(String []args, int index, String defaultValue) 
	{
		if( index < args.length ) {
			return args[index];
		} else {
			return defaultValue;
		}
	}
}
