package json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import database.QueueSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Clob;
import java.sql.SQLException;

public class ClobFieldSerializer extends JsonSerializer<Clob> {
    public static Logger logger = LoggerFactory.getLogger(QueueSetup.class);

    @Override
    public void serialize(Clob clobValue, JsonGenerator jsonGenerator, SerializerProvider serializerProvider){
        try {
            String s = clobValue.getSubString(1, (int) clobValue.length());
            jsonGenerator.writeString(s);
        } catch (SQLException e) {
            logger.error("SQLException:\n" + e.getMessage());
        } catch (IOException e) {
            logger.error("IOException:\n" + e.getMessage());
        }
    }
}
