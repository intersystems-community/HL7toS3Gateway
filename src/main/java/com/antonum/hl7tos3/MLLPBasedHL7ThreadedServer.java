package com.antonum.hl7tos3;

import java.io.*;
import java.net.*;
import java.util.*;
	
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import static java.nio.charset.StandardCharsets.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

//import org.apache.commons.cli.*;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

	public class MLLPBasedHL7ThreadedServer {
		
		private int maxConnections;
		private int listenPort;
		
		//private static String connectionString = "{connection string}";
		//private static String queueName = "{queue name}";
		//private static final String queueName = "adtfromepic";
		//private static IQueueClient queueClient;

	
		
		public MLLPBasedHL7ThreadedServer(int aListenPort, int maxConnections) {
	        listenPort = aListenPort;
	        this.maxConnections = maxConnections;
	     }
	     
          public void acceptConnections() {
	        try {
	            ServerSocket server = new ServerSocket(listenPort, 5);
	            Socket clientSocket = null;
	            while (true) {
	                clientSocket = server.accept();
	                handleConnection(clientSocket);
	            }
	        } catch (BindException e) {
	            System.out.println("Unable to bind to port " + listenPort);
	        } catch (IOException e) {
	            System.out.println("Unable to instantiate a ServerSocket on port: " + listenPort);
	        }
	     }
	    
	     protected void handleConnection(Socket connectionToHandle) {
	        ConnectionHandler.processRequest(connectionToHandle);
	     }
	    
	     public static void main(String[] args) {

	        System.out.println("Starting MLLP server on port 1080");
			MLLPBasedHL7ThreadedServer server = new MLLPBasedHL7ThreadedServer(1080, 3);
	        server.setUpConnectionHandlers();
	        server.acceptConnections();
	     }
	     public static void uploadFileToS3(String file_path, String bucket_name, String key_name) {
			System.out.format("Uploading %s to S3 bucket %s...\n", key_name, bucket_name);
			final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
			try {
				s3.putObject(bucket_name, key_name, new File(file_path));
			} catch (AmazonServiceException e) {
				System.err.println(e.getErrorMessage());
				System.exit(1);
			}
		 }
	     public void setUpConnectionHandlers() {
	        for (int i = 0; i < maxConnections; i++) {
	            ConnectionHandler currentHandler =
	              new ConnectionHandler();
	            Thread handlerThread = new Thread(currentHandler, "Handler " + i);
	            handlerThread.setDaemon(true);
	            handlerThread.start();
	        }
	     }

	     private static class ConnectionHandler implements Runnable {
	          private static final int MESSAGE_CONTROL_ID_LOCATION = 9;
			private static final String FIELD_DELIMITER = "|";
			private Socket connection;
	          private static List pool = new LinkedList();
	          static final char END_OF_BLOCK = '\u001c'; 
	    	     static final char START_OF_BLOCK = '\u000b';
	    	     static final char CARRIAGE_RETURN = 13; 
	    	     private static final int END_OF_TRANSMISSION = -1;
	        
	          public ConnectionHandler() {
	          }
	        
               public void handleConnection() {
	        	 try {
	            	System.out.println("Handling client at " 
	            	+ connection.getInetAddress().getHostAddress() 
	            	+ " on port " + connection.getPort());
	
	            	InputStream in = connection.getInputStream();
	            	OutputStream out = connection.getOutputStream();
	
	            	String parsedHL7Message = getMessage(in);
					System.out.println(parsedHL7Message);
					writeHL7File(parsedHL7Message);
					UUID uuid = UUID.randomUUID();
					String key_name=uuid.toString()+".hl7";
					uploadFileToS3("filename.txt",System.getenv("S3_BUCKET"),key_name);
					//sendMessageToQueue(parsedHL7Message);
					//System.out.println(parsedHL7Message);
	            	String buildAcknowledgmentMessage = getSimpleAcknowledgementMessage(parsedHL7Message);
	            	out.write(buildAcknowledgmentMessage.getBytes(), 0, buildAcknowledgmentMessage.length());

	        	 } catch (IOException e) {
				String errorMessage = "Error whiling reading and writing to connection " + e.getMessage();
				System.out.print(errorMessage);
				throw new RuntimeException(errorMessage);
			 }	      
	            
	           finally {
	              try {
				  connection.close();
				} 
	              catch (IOException e) {
				  String errorMessage = "Error whiling attempting to close to connection " + e.getMessage();
				  System.out.println(errorMessage);
				  throw new RuntimeException(errorMessage);
	            	}	
	            }
	        }
	        public static void writeHL7File(String message) {
				try {
					FileWriter myWriter = new FileWriter("filename.txt");
					myWriter.write(message);
					myWriter.close();
					//System.out.println("Successfully wrote to the file.");
				  } catch (IOException e) {
					System.out.println("An error occurred.");
					e.printStackTrace();
				  }
				
			}
			
			public static void processRequest(Socket requestToHandle) {
	            synchronized (pool) {
	                pool.add(pool.size(), requestToHandle);
	                pool.notifyAll();
	             }
	           }

	           public void run() {
	             while (true) {
	                synchronized (pool) {
	                    while (pool.isEmpty()) {
	                        try {
	                            pool.wait();
	                        } catch (InterruptedException e) {
	                            return;
	                        }
	                    }
	                    connection = (Socket) pool.remove(0);
	                }
	                handleConnection();
	             }
	           }
	        
	          public String getMessage(InputStream anInputStream) throws IOException  {
	       	 	        	
	    			boolean end_of_message = false;
				StringBuffer parsedMessage = new StringBuffer();
	
				int characterReceived = 0;
	
				try {
					characterReceived = anInputStream.read();
				} catch (SocketException e) {
					System.out
					.println("Unable to read from socket stream. "
					+ "Connection may have been closed: " + e.getMessage());
					return null;
				}
	
				if (characterReceived == END_OF_TRANSMISSION) {
					return null;
				}
	
				if (characterReceived != START_OF_BLOCK) {
					throw new RuntimeException(
					"Start of block character has not been received");
				}
	
				while (!end_of_message) {
					characterReceived = anInputStream.read();
						
	
					if (characterReceived == END_OF_TRANSMISSION) {
						throw new RuntimeException(
						"Message terminated without end of message character");
					}
	
					if (characterReceived == END_OF_BLOCK) {
						characterReceived = anInputStream.read();
	
						if (characterReceived != CARRIAGE_RETURN) {
							throw new RuntimeException(
							"End of message character must be followed by a carriage return character");
						}
						end_of_message = true;
						//System.out.println();
					} else {
						parsedMessage.append((char) characterReceived);
						//System.out.print(" "+(char)characterReceived);
					}
				}
	
				return parsedMessage.toString();
	    	     }
	    	   
	    	    private String getSimpleAcknowledgementMessage(String aParsedHL7Message) {
	    		 if (aParsedHL7Message == null)
	    			throw new RuntimeException("Invalid HL7 message for parsing operation" +
	    					". Please check your inputs");
	    		   		
	    		 String messageControlID = getMessageControlID(aParsedHL7Message);
	    		
	    		 StringBuffer ackMessage = new StringBuffer();
	    		 ackMessage = ackMessage.append(START_OF_BLOCK)
	    		                       .append("MSH|^~\\&|||||||ACK||P|2.2") 
	    		                       .append(CARRIAGE_RETURN)
	    		                       .append("MSA|AA|")
	    		                       .append(messageControlID)
	    		                       .append(CARRIAGE_RETURN)
	    		                       .append(END_OF_BLOCK)
	    		                       .append(CARRIAGE_RETURN);
	    		
	    		 return ackMessage.toString();
	    	    }
	    	
		   private String getMessageControlID(String aParsedHL7Message) {
			  int fieldCount = 0;
			  StringTokenizer tokenizer = new StringTokenizer(aParsedHL7Message, FIELD_DELIMITER);
				
			  while (tokenizer.hasMoreElements())
		        {
		            String token = tokenizer.nextToken();
		            fieldCount++;
		            if (fieldCount == MESSAGE_CONTROL_ID_LOCATION){
		            	return token;
		            }		            
		        }
				
				return "";	
		   }
	        
	    }        
	    
	}
