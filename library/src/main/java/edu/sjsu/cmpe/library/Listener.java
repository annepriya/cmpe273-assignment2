package edu.sjsu.cmpe.library;

import java.net.MalformedURLException;
import java.net.URL;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;




import org.fusesource.stomp.jms.StompJmsDestination;




import edu.sjsu.cmpe.library.domain.Book;
import edu.sjsu.cmpe.library.domain.Book.Status;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;

public class Listener implements Runnable {
	
	 private String stompTopicName;
	 private Connection connection;
	 private final BookRepositoryInterface bookRepository;
	
	public Listener(BookRepositoryInterface bookInterface,String topicName,Connection conn){
		this.stompTopicName=topicName;
		this.connection=conn;
		this.bookRepository=bookInterface;
		
	}
	
	 public String getStompTopicName() {
		return stompTopicName;
	}

	public void setStompTopicName(String stompTopicName) {
		this.stompTopicName = stompTopicName;
	}

	
	
	public void run() {

			String[] tokens=null;
			try {
				
				connection.start();
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
				String destination=getStompTopicName();
				Destination dest = new StompJmsDestination(destination);

				MessageConsumer consumer = session.createConsumer(dest);
				System.currentTimeMillis();
				System.out.println("Waiting for messages...in topic");
				while(true) {
				    Message msg = consumer.receive();
				    if( msg instanceof  TextMessage ) {
					String body = ((TextMessage) msg).getText();
					if( "SHUTDOWN".equals(body)) {
					    break;
					}
					tokens=body.split(":");
					System.out.println("tokens isbn"+tokens[0]);
					Book book=bookRepository.getBookByISBN(Long.parseLong(tokens[0]));
					if(book!=null){
						book.setStatus(Status.available);
						
					}else{
						Book newBook=new Book();
						newBook.setIsbn(Long.parseLong(tokens[0]));
						newBook.setTitle(tokens[1]);
						newBook.setCategory(tokens[2]);
						newBook.setCoverimage(new URL(tokens[3]+":"+tokens[4]));
						bookRepository.saveBook(newBook);
						
					}
					System.out.println("Received message = " + body);
					
						
				
					//connection.close();

				    } else {
					System.out.println("Unexpected message type: "+msg.getClass());
				    }
			} 
			
				connection.close();
			}catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	}
	


}
