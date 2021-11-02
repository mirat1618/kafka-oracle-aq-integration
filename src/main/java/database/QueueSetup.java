package database;

import oracle.AQ.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueSetup {
    public static Logger logger = LoggerFactory.getLogger(QueueSetup.class);
    private static AQSession aqSession;

    static {
        aqSession = QueueManager.getAqSession();
    }

    public static void main(String[] args) {
        createQueue("C##USER", "EXTENDED_SONGS_TABLE", "EXTENDED_SONGS_QUEUE", "C##USER.EXTENDED_SONG");
        startQueue("C##USER", "EXTENDED_SONGS_QUEUE");
//        stopQueue("C##USER", "EXTENDED_SONGS_QUEUE");
    }

    public static void createQueue(String schemaName, String tableName, String queueName, String objectName) {
        try {
            AQQueueTableProperty  queueTableProperty = new AQQueueTableProperty(objectName);

            AQQueueTable  queueTable = aqSession.createQueueTable(schemaName, tableName, queueTableProperty);
            logger.info(tableName + " queue table has been successfully created in " + schemaName + " schema");

            AQQueueProperty queueProperty = new AQQueueProperty();

            AQQueue queue = aqSession.createQueue(queueTable, queueName, queueProperty);
            logger.info(queueName + " queue has been successfully created in " + schemaName + " schema" + " for the " + objectName + " object");
        } catch(AQException e) {
            logger.error("AQException:\n" + e.getMessage());
        }
    }

    public static void startQueue(String schemaName, String queueName) {
        try {
            AQQueue queue = aqSession.getQueue(schemaName, queueName);
            queue.start();

            logger.info(queueName + " queue has been started");
        } catch (AQException e) {
            logger.error("AQException:\n" + e.getMessage());
        }
    }

    public static void stopQueue(String schemaName, String queueName) {
        try {
            AQQueue queue = aqSession.getQueue(schemaName, queueName);
            queue.stop(true);

            logger.info(queueName + " queue has been stopped");
        } catch (AQException e) {
            logger.error("AQException:\n" + e.getMessage());
        }
    }
}
