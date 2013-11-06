package edu.sjsu.cmpe.library;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.views.ViewBundle;

import edu.sjsu.cmpe.library.api.resources.BookResource;
import edu.sjsu.cmpe.library.api.resources.RootResource;
import edu.sjsu.cmpe.library.api.resources.TopicListener;
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
	}

	@Override
	public void run(LibraryServiceConfiguration configuration,
			Environment environment) throws Exception {
		// This is how you pull the configurations from library_x_config.yml
		String queueName = configuration.getStompQueueName();
		final String topicName = configuration.getStompTopicName();
		String libName = configuration.getLibName();

		String aUser = configuration.getApolloUser();
		String aPwd = configuration.getApolloPassword();
		String aHost = configuration.getApolloHost();
		String aPort = configuration.getApolloPort();

		log.debug("Queue name is {}. Topic name is {}. Lib name {}", queueName,
				topicName, libName);
		log.debug("APOLLO_USER {}. APOLLO_PASSWORD {}. APOLLO_HOST {}. APOLLO_PORT {}q	", aUser,
				aPwd, aHost, aPort);


		// TODO: Apollo STOMP Broker URL and login

		String user = env("APOLLO_USER", aUser);
		String password = env("APOLLO_PASSWORD", aPwd);
		String host = env("APOLLO_HOST", aHost);
		int port = Integer.parseInt(env("APOLLO_PORT", aPort));
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);

		final Connection connection = factory.createConnection(user, password);
		connection.start();

		//Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		/** Root API */
		environment.addResource(RootResource.class);
		/** Books APIs */
		final BookRepositoryInterface bookRepository = new BookRepository();
		environment.addResource(new BookResource(bookRepository, connection, queueName, libName, topicName));


		int numThreads = 1;
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);

		Runnable backgroundTask = new Runnable() {

			@Override
			public void run() {
				System.out.println("Update Library");
				TopicListener listener = new TopicListener (bookRepository,topicName, connection);
				try {
					listener.listenToTopic();
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};

		//System.out.println("About to submit the background task");
		executor.execute(backgroundTask);
		//System.out.println("Submitted the background task");

		 executor.shutdown();
		 //System.out.println("Finished the background task");

		/** UI Resources */
		//environment.addResource(new HomeResource(bookRepository));
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
