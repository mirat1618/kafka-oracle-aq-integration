package database;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import json.JsonValidator;
import model.ExtendedSong;
import oracle.AQ.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class QueueManager {
    private static Logger logger = LoggerFactory.getLogger(QueueManager.class);
    private static ClassLoader loader = Thread.currentThread().getContextClassLoader();
    public static ObjectMapper objectMapper = new ObjectMapper();

    private static String JDBC_URL;
    private static String JDBC_USERNAME;
    private static String JDBC_PASSWORD;
    private static boolean JDBC_AUTOCOMMIT;
    private static Class<ExtendedSong> objectSQLDataClass = ExtendedSong.class;
    private static String objectSQLDataClassName = "model.ExtendedSong";

    private static Connection connection = null;
    private static AQSession aqSession = null;

    private static AQQueue queue;
    private static AQEnqueueOption enqueueOption;
    private static AQDequeueOption dequeueOption;
    private static AQMessage message;

    private static String schemaName = "C##USER";
    private static String queueName = "EXTENDED_SONGS_QUEUE";
    private static String queueTableName;
    private static String objectName = "EXTENDED_SONG";

    static {
        readJdbcProperties();
        establishDatabaseConnection();
        createAqSession();
        setQueue();
    }

    private static void readJdbcProperties() {
        Properties jdbcProperties = new Properties();

        try(InputStream resourceStream = loader.getResourceAsStream("jdbc.properties")) {
            jdbcProperties.load(resourceStream);
        } catch (IOException e) {
            logger.error("IOException:\n" + e.getMessage());
        }

        JDBC_URL = jdbcProperties.getProperty("JDBC_URL");
        JDBC_USERNAME = jdbcProperties.getProperty("JDBC_USERNAME");
        JDBC_PASSWORD = jdbcProperties.getProperty("JDBC_PASSWORD");
        JDBC_AUTOCOMMIT = Boolean.parseBoolean(jdbcProperties.getProperty("JDBC_AUTOCOMMIT"));
    }

    private static void establishDatabaseConnection() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD);
            connection.setAutoCommit(JDBC_AUTOCOMMIT);
        } catch (SQLException e) {
            logger.error("SQLException:\n" + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException:\n" + e.getMessage());
        }
    }

    private static void createAqSession() {
        try {
            Class.forName("oracle.AQ.AQOracleDriver");
            aqSession = AQDriverManager.createAQSession(connection);
        } catch (AQException e) {
            logger.error("AQException:\n" + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException:\n" + e.getMessage());
        }
    }

    public static Connection getConnection() {
        return connection;
    }

    public static AQSession getAqSession() {
        return aqSession;
    }

    private static void setQueue() {
        try {
            queue = aqSession.getQueue(schemaName, queueName);
            enqueueOption = new AQEnqueueOption();
            dequeueOption = new AQDequeueOption();
            queueTableName = queue.getQueueTableName();
        } catch (AQException e) {
            logger.error("AQException:\n" + e.getMessage());
        }
    }

    public static boolean enqueue(String string) {
        ExtendedSong song;

        if (string == null) {
            logger.error("The passed string is null");
            return false;
        }

        boolean isValidJson = JsonValidator.validate(string);

        if (isValidJson) {
            try {
                song = objectMapper.readValue(string, objectSQLDataClass); // converting JSON string to an object
                logger.info("JSON string has been deserialized: " + song);
            } catch (JsonProcessingException e) {
                logger.error("JsonProcessingException:\n" + e.getMessage());
                return false;
            }

            try {
                message = queue.createMessage();

                Object[] songProperties = {
                        song.getTitle(),
                        song.getDuration(),
                        song.getFilePath(),
                        song.getDescription(),
                        song.getAddedAt()
                };

                Struct struct = connection.createStruct(objectName, songProperties);
                AQObjectPayload payload = message.getObjectPayload();
                payload.setPayloadData(struct);

                queue.enqueue(enqueueOption, message);

                logger.info("The object has been enqueued - " + song + " - to " + queueName);

                connection.commit();
            } catch (AQException e) {
                logger.error("AQException:\n" + e.getMessage());
                return false;
            } catch (SQLException e) {
                logger.error("SQLException:\n" + e.getMessage());
                return false;
            }
        }
        return true;
    }

    public static String dequeue() {
        ExtendedSong song = null;
        String songJson = null;
        try {
            if (!queueIsEmpty()) {
                AQMessage message = queue.createMessage();
                try {
                    message = queue.dequeue(dequeueOption, Class.forName(objectSQLDataClassName));
                } catch (ClassNotFoundException e) {
                    logger.error("ClassNotFoundException:\n" + e.getMessage());
                }
                AQObjectPayload payload = message.getObjectPayload();
                song = (ExtendedSong) payload.getPayloadData();
                logger.info("The object has been dequeued - " + song + " - from " + queueName);

                connection.commit();

                try {
                    songJson = objectMapper.writeValueAsString(song);
                } catch (JsonProcessingException e) {
                    logger.error("JsonProcessingException:\n" + e.getMessage());
                }
            }
        } catch (AQException e) {
            logger.error("AQException:\n" + e.getMessage());
        } catch (SQLException e) {
            logger.error("SQLException:\n" + e.getMessage());
        }
        return songJson;
    }

    private static boolean queueIsEmpty() {
        String query = "SELECT COUNT (*) FROM " + queueTableName;
        int queueObjectsNumber = 0;
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                queueObjectsNumber = Integer.parseInt(resultSet.getString("COUNT(*)"));
            }
        } catch (SQLException e) {
            logger.error("SQLException:\n" + e.getMessage());
        }
        return queueObjectsNumber == 0;
    }
}
