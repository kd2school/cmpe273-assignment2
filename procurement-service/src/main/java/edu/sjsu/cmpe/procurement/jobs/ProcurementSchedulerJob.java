package edu.sjsu.cmpe.procurement.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.Consumer;
import edu.sjsu.cmpe.procurement.ProcurementService;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;

/**
 * Class: ProcurementSchedulerJob 
 * This job will run at every 5 secondss.
 */

@Every("2min")
public class ProcurementSchedulerJob extends Job {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void doJob() {

    	
    	String user = env("APOLLO_USER", "admin");
		String password = env("APOLLO_PASSWORD", "password");
		String host = env("APOLLO_HOST", "54.215.210.214");
		int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
		
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);
		
		System.out.println("WakeUp -- Execute job");
		
		Connection connection;
		
		try {
			connection = factory.createConnection(user, password);
			connection.start();
			Consumer consumer = new Consumer(connection, ProcurementService.queueName, ProcurementService.postPublisher, 
					ProcurementService.getPublisher, ProcurementService.topicName);
			consumer.listenToQueue();
		
			connection.close();
		
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
    	
    	
    }
    
	private static String env(String key, String defaultValue) 
	{
		String rc = System.getenv(key);
		if( rc== null ) {
			return defaultValue;
		}
		return rc;
	}
}
