package edu.sjsu.cmpe.procurement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Connection;
import javax.ws.rs.core.MediaType;

import java.util.regex.Pattern;

import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.WebResource;

public class Consumer 
{
	private final Logger log = LoggerFactory.getLogger(getClass());
	private final Connection connection;
	//private final Session session;
	private final String queueName;
	private final String id = "15512";
	private List<Integer> isbns = new ArrayList<Integer>();
	private List<String> msgList = new ArrayList<String>();
	//private List<String> msgListB = new ArrayList<String>();
	private final WebResource postConnection;
	private final WebResource getConnection;
	private final String rootTopic;
	//private final String topicB;


	public Consumer(Connection connection, String queueName, WebResource postConnection,
			WebResource getConnection, String rootTopic)
			{
		this.connection = connection;	
		this.queueName = queueName;
		this.postConnection = postConnection;
		this.getConnection = getConnection;
		this.rootTopic = rootTopic;
		//this.topicB = topicB;
			}

	public void parse(String message)
	{
		//System.out.println("Inside parse()");
		String delims = "[:]";
		String [] tokens = message.split(delims); 

//		for(String str: tokens)
//			System.out.println(str);

		for(String str: tokens)
		{
			if (Pattern.compile("-?[0-9]+").matcher(str).matches()) {
				isbns.add(Integer.parseInt(str));
				//log.debug("add isbn to array: " + Integer.parseInt(str));

			}
		}
	}


	public boolean doPost()
	{
		if( isbns.isEmpty())
			return false;

		Map map = new HashMap();
		map.put("id", id);
		map.put("order_book_isbns", isbns);
		String match ="successfully";

		System.out.println(map);

		String response = postConnection.accept(MediaType.APPLICATION_JSON_TYPE).
				header("X-FOO", "BAR").
				type(MediaType.APPLICATION_JSON_TYPE).
				post(String.class, map);

		System.out.println(response);

		if( response.toLowerCase().contains(match.toLowerCase()) )
			return true;

		return false;


	}


	public boolean doGet()
	{	
		HashMap response = getConnection.accept(MediaType.APPLICATION_JSON_TYPE).
				header("X-FOO", "BAR").
				type(MediaType.APPLICATION_JSON_TYPE).
				get(HashMap.class);

		if( response == null)
			return false;

		ArrayList<HashMap> cursor = (ArrayList<HashMap>) response.get("shipped_books");

		for(HashMap map: cursor)
		{

			String msg = map.get("isbn") + ":\"" + map.get("title") + "\":\"" + 
					map.get("category") + "\":\"" + map.get("coverimage") + "\""; 

			System.out.println(msg);
			msgList.add(msg);


		}

		return true;

	}


	public MessageProducer createProducer(String topic) throws JMSException
	{
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(topic);
		//Destination destB = new StompJmsDestination(topicB);

		MessageProducer	producer = session.createProducer(dest);
		//MessageProducer producerB = session.createProducer(destB);

		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		//producerB.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

		return producer;


	}


	public MessageProducer createTopicProducer(Session session, String topic) throws JMSException
	{

		Destination dest = new StompJmsDestination(rootTopic + topic);

		MessageProducer producer = session.createProducer(dest);

		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

		return producer;
	}


	public void publishToTopics()
	{
		// computer, comics, management, and selfimprovement
		String computer = "computer";
		String comics = "comics";
		String management = "management";
		String selfimprovement = "selfimprovement";


		Session session;

		try {
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);


			for(String data: msgList)
			{
				TextMessage msg = session.createTextMessage(data);
				msg.setLongProperty("id", System.currentTimeMillis());

				if( data.toLowerCase().contains(computer.toLowerCase()) )
				{
					MessageProducer producer = createTopicProducer(session, computer);
					System.out.println("Sending messages to " + computer +"...");
					System.out.println(data);
					
					producer.send(msg);
				}

				else if(data.toLowerCase().contains(comics.toLowerCase()))
				{
					MessageProducer producer = createTopicProducer(session, comics);
					System.out.println("Sending messages to " + comics +"...");
					System.out.println(data);
					
					producer.send(msg);
				}
				else if (data.toLowerCase().contains(management.toLowerCase()))
				{
					MessageProducer producer = createTopicProducer(session, management);
					System.out.println("Sending messages to " + management +"...");
					System.out.println(data);
					
					producer.send(msg);

				}
				else if (data.toLowerCase().contains(selfimprovement.toLowerCase()))
				{ 
					MessageProducer producer = createTopicProducer(session, selfimprovement);
					System.out.println("Sending messages to " + selfimprovement +"...");
					System.out.println(data);
					
					producer.send(msg);

				}

			}

		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



	public void printISBN()
	{
		for(int i: isbns)
		{
			System.out.print(i + " ");
		}

	}


	public void listenToQueue() throws JMSException, InterruptedException 
	{
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(queueName);

		MessageConsumer consumer = session.createConsumer(dest);

		int timeout = 10000;

		System.out.println("Waiting for messages from " + queueName + "...");
		
		while(true) 
		{
			
			Message msg = consumer.receive(timeout);  // timed out after 2 seconds

			if( msg instanceof  TextMessage ) 
			{
				String body = ((TextMessage) msg).getText();

				parse(body);

			}	
			else if(msg == null)
			{

				if(doPost())
				{	
					isbns.clear();
					System.out.println("Posted");
				}

				if(doGet())
				{
					publishToTopics();
					msgList.clear();
				}	
			

				System.out.println("sleeping...");
			
				break;
			}

			else {

				System.out.println("Unexpected message type: " + msg.getClass());

			}			

		}
	}

}
