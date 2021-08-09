package com.antonum.hl7tos3;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.BindException;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.UUID;
import java.util.StringTokenizer;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MLLPBasedHL7ThreadedServer {

    private int maxConnections;
    private int listenPort;

    // S3 client
    final static private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

    // S3 bucket
    final static private String s3_bucket = System.getenv("S3_BUCKET");
    private static final Logger logger = LogManager.getLogger(MLLPBasedHL7ThreadedServer.class);


    public MLLPBasedHL7ThreadedServer(int listenPort, int maxConnections) {
        this.listenPort = listenPort;
        this.maxConnections = maxConnections;
    }

    public static void main(String[] args) {
        // Determine port to run. Defaults to 1080
        String listenPortArg = "1080";
        try {
            listenPortArg = args[0];
        } catch (ArrayIndexOutOfBoundsException e){
            listenPortArg = "1080";
        }

        int listenPort;
        try {
            listenPort = Integer.parseInt(listenPortArg);
        } catch (NumberFormatException e) {
            logger.warn("Supplied invalid port number: {}. Using 1080 instead.", listenPortArg);
            listenPort = 1080;
        }

        logger.info("Starting MLLP server on port {}", listenPort);
        logger.info("S3 BUCKET: {}", System.getenv("S3_BUCKET"));

        // Start TCP server
        MLLPBasedHL7ThreadedServer server = new MLLPBasedHL7ThreadedServer(listenPort, 3);
        server.setUpConnectionHandlers();
        server.acceptConnections();
    }

    public static void uploadStringToS3(String content, String key_name) {
        logger.info("Uploading {} to S3 bucket {}", key_name, s3_bucket);
        try {
            s3.putObject(s3_bucket, key_name, content);
        } catch (AmazonServiceException e) {
            logger.fatal("Error uploading to S3", e);
            System.exit(1);
        }
        logger.info("Success!\n");
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
            logger.error("Unable to bind to port: {}", listenPort);
        } catch (IOException e) {
            logger.error("Unable to instantiate a ServerSocket on port: {} ", listenPort);
        }
    }

    protected void handleConnection(Socket connectionToHandle) {
        ConnectionHandler.processRequest(connectionToHandle);
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
        static final char END_OF_BLOCK = '\u001c';
        static final char START_OF_BLOCK = '\u000b';
        static final char CARRIAGE_RETURN = 13;
        private static final int MESSAGE_CONTROL_ID_LOCATION = 9;
        private static final String FIELD_DELIMITER = "|";
        private static final int END_OF_TRANSMISSION = -1;
        private static LinkedList pool = new LinkedList();
        private Socket connection;


        public ConnectionHandler() {
        }

        public static void processRequest(Socket requestToHandle) {
            synchronized (pool) {
                pool.add(pool.size(), requestToHandle);
                pool.notifyAll();
            }
        }

        // Main method. Converts incoming stream into String and uploads it to S3
        public void handleConnection() {
            try {
                logger.info("Handling client at {} on port {}", connection.getInetAddress().getHostAddress(), connection.getPort());

                InputStream in = connection.getInputStream();
                OutputStream out = connection.getOutputStream();

                String parsedHL7Message = getMessage(in);
                logger.trace(parsedHL7Message);

                UUID uuid = UUID.randomUUID();
                String key_name = uuid.toString() + ".hl7";
                uploadStringToS3(parsedHL7Message, key_name);

                String buildAcknowledgmentMessage = getSimpleAcknowledgementMessage(parsedHL7Message);
                out.write(buildAcknowledgmentMessage.getBytes(), 0, buildAcknowledgmentMessage.length());

            } catch (IOException e) {
                String errorMessage = "Error while reading and writing to connection " + e.getMessage();
                logger.error(errorMessage, e);
                throw new RuntimeException(errorMessage);
            } finally {
                try {
                    connection.close();
                } catch (IOException e) {
                    String errorMessage = "Error while attempting to close to connection " + e.getMessage();
                    logger.error(errorMessage, e);
                    throw new RuntimeException(errorMessage);
                }
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

        public String getMessage(InputStream anInputStream) throws IOException {

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
                } else {
                    parsedMessage.append((char) characterReceived);
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

            while (tokenizer.hasMoreElements()) {
                String token = tokenizer.nextToken();
                fieldCount++;
                if (fieldCount == MESSAGE_CONTROL_ID_LOCATION) {
                    return token;
                }
            }

            return "";
        }

    }

}
