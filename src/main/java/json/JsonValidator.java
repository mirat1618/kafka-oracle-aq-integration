package json;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonValidator {
    public static ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    public static JSONObject jsonSchema = new JSONObject(new JSONTokener(classLoader.getResourceAsStream("SongExtendedJsonSchema.json")));
    public static Schema schema = SchemaLoader.load(jsonSchema);
    public static Logger logger = LoggerFactory.getLogger(JsonValidator.class);

    public static boolean validate(String jsonString) {
        boolean isValid;
        try {
            JSONObject jsonObject = new JSONObject(new JSONTokener(jsonString));
            schema.validate(jsonObject);
            isValid = true;
        } catch(ValidationException e) {
            isValid = false;
            logger.error("ValidationException:\n" + e.getMessage());
        } catch (JSONException e) {
            isValid = false;
            logger.error("JSONException:\n" + e.getMessage());
        }
        return isValid;
    }
}
