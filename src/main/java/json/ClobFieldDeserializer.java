package json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import database.QueueManager;
import database.QueueSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;

public class ClobFieldDeserializer extends JsonDeserializer<Clob> {
    public static Logger logger = LoggerFactory.getLogger(QueueSetup.class);

    @Override
    public Clob deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {
        String string = null;
        try {
            string = jsonParser.getText();
        } catch (IOException e) {
            logger.error("SQLException:\n" + e.getMessage());
            return null;
        }
        try {
            Connection connection = QueueManager.getConnection();
            Clob clob = connection.createClob();

            clob.setString(1, string);
            return clob;
        } catch (SQLException e) {
            logger.error("SQLException:\n" + e.getMessage());
            return null;
        }
    }
}
