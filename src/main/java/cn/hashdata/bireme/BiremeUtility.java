package cn.hashdata.bireme;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map.Entry;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import cn.hashdata.bireme.Config.ConnectionConfig;

public class BiremeUtility {
  /**
   * Establish connection to database.
   *
   * @param conf configuration of the aimed database
   * @return the established connection
   * @throws BiremeException Failed to get connection
   */
  public static Connection jdbcConn(ConnectionConfig conf) throws BiremeException {
    Connection conn = null;

    try {
      conn = DriverManager.getConnection(conf.jdbcUrl, conf.user, conf.passwd);
    } catch (SQLException e) {
      throw new BiremeException("Fail to get connection.\n", e);
    }

    return conn;
  }

  /**
   * Given the key, return the json value as String, ignoring case considerations.
   * @param data the JsonObject
   * @param fieldName the key
   * @return the value as String
   * @throws BiremeException when the JsonObject doesn't have the key
   */
  public static String jsonGetIgnoreCase(JsonObject data, String fieldName) throws BiremeException {
    JsonElement element = data.get(fieldName);

    if (element == null) {
      for (Entry<String, JsonElement> iter : data.entrySet()) {
        String key = iter.getKey();

        if (key.equalsIgnoreCase(fieldName)) {
          element = iter.getValue();
          break;
        }
      }
    }

    if (element == null) {
      throw new BiremeException(
          "Not found. Record does not have a field named \"" + fieldName + "\".\n");
    }

    if (element.isJsonNull()) {
      return null;
    } else {
      return element.getAsString();
    }
  }
}
