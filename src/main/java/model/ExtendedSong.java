package model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import database.QueueSetup;
import json.ClobFieldDeserializer;
import json.ClobFieldSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class ExtendedSong implements SQLData {
    public static Logger logger = LoggerFactory.getLogger(QueueSetup.class);

    private String sql_type;
    private String title;
    private int duration;
    private String filePath;
    private Clob description;
    @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="yyyy-MM-dd HH:mm:ss.SS")
    private Timestamp addedAt;

    public ExtendedSong() {}

    public ExtendedSong(String title, int duration, String filePath, Clob description, Timestamp addedAt) {
        this.title = title;
        this.duration = duration;
        this.filePath = filePath;
        this.description = description;
        this.addedAt = addedAt;
    }

    @JsonIgnore
    public String getSQLTypeName() throws SQLException {
        return sql_type;
    }

    public void readSQL(SQLInput stream, String typeName)  {
        sql_type = typeName;
        try {
            this.title = stream.readString();
            this.duration = stream.readInt();
            this.filePath = stream.readString();
            this.description = stream.readClob();
            this.addedAt = stream.readTimestamp();
        } catch(SQLException e) {
            logger.error("SQLException:\n" + e.getMessage());
        }
    }

    public void writeSQL(SQLOutput stream) throws SQLException {
        try {
            stream.writeString(title);
            stream.writeInt(duration);
            stream.writeString(filePath);
            stream.writeClob(description);
            stream.writeTimestamp(addedAt);
        } catch(SQLException e) {
            logger.error("SQLException:\n" + e.getMessage());
        }
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    @JsonSerialize(using = ClobFieldSerializer.class)
    public Clob getDescription() {
        return description;
    }

    @JsonDeserialize(using = ClobFieldDeserializer.class)
    public void setDescription(Clob description) {
        this.description = description;
    }

    public Timestamp getAddedAt() {
        return addedAt;
    }

    public void setAddedAt(Timestamp addedAt) {
        this.addedAt = addedAt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\u001B[34m");  // \u001B[34m is a blue font color for UNIX terminal
        sb.append("SongExtended{");
        sb.append("title='").append(title).append('\'');
        sb.append(", duration=").append(duration);
        sb.append(", filePath='").append(filePath).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", addedAt='").append(addedAt).append('\'');
        sb.append("}");
        sb.append("\u001b[0m"); // \u001b[0m resetting terminal's font color back to default

        return sb.toString();
    }

//    public Object[] toObjectArray() {
//        Object objectFields[] = {
//                this.getTitle(),
//                this.getDuration(),
//                this.getFilePath(),
//                this.getDescription(),
//                this.getAddedAt()
//        };
//        return objectFields;
//    }
}
