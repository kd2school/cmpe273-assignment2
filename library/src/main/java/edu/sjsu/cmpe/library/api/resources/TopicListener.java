package edu.sjsu.cmpe.library.api.resources;


import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;

import edu.sjsu.cmpe.library.domain.Book;
import edu.sjsu.cmpe.library.domain.Book.Status;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;

public class TopicListener {

	private BookRepositoryInterface bookRepository;
	private String topicName;
	private Connection connection;

	public TopicListener(BookRepositoryInterface bookRepository, 
			String topicName, Connection connection) {
		this.bookRepository = bookRepository;
		this.connection = connection;
		this.topicName = topicName;
	}


	public void updateRepository(String line)
	{
		//System.out.println(line);

		line = line.replaceAll("\"", "");
		String[] tokens = line.split(":");

		if (tokens.length != 5)
			System.out.println("No Match");

		long isbn = Long.parseLong(tokens[0]);
		String title = tokens[1];
		String category = tokens[2];
		String URI = tokens[3]+ ":" + tokens[4];

//		System.out.println(isbn);
//		System.out.println(title);
//		System.out.println(category);
//		System.out.println(URI);

		Book book = bookRepository.getBookByISBN(isbn);
		
		if(book == null)
		{
			Book newBook = new Book();
			newBook.setIsbn(isbn);
			newBook.setTitle(title);
			newBook.setCategory(category);
			
			try {
				newBook.setCoverimage(new URL(URI));
			} catch (MalformedURLException e) {
				// eat the exception
			    e.printStackTrace();
			}
			
			bookRepository.saveBook(newBook);
		}
		
		else {
			 book.setStatus(Status.available);
		}

//		List<Book> books = bookRepository.getAllBooks();

//		for(Book b: books)
//			b.printBook();

	}


	public void listenToTopic() throws JMSException
	{
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(topicName);

		MessageConsumer consumer = session.createConsumer(dest);
		System.currentTimeMillis();
		System.out.println("Waiting for messages from " + topicName + "...");

		while(true) 
		{
			Message msg = consumer.receive(2000);  // timed out after 2 seconds
			//msg.acknowledge();
			
			if(msg == null)
			{	

				continue;
			}	

			//System.out.println("receive messages ...");
			else if( msg instanceof  TextMessage ) 
			{
				String body = ((TextMessage) msg).getText();
				
				System.out.println("Received message = " + body);
				updateRepository(body);
				//System.out.println("Received TextMessage = " + body);
				

			} else {
				System.out.println("Unexpected message type: "+msg.getClass());
			}


		}

	}
}
