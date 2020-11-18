package dao;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ReadDao {

  private static final Logger logger = Logger.getLogger(ReadDao.class);

  private static final String READ_DATA =
      "SELECT USERS.ID, USERS.NAME, USERS.SURNAME, USERS.BIRTHDATE\n"
          + ", (SELECT COUNT(FRIENDSHIPS.USERID1) \n"
          + "FROM FRIENDSHIPS \n"
          + "WHERE FRIENDSHIPS.USERID1 = USERS.ID \n"
          + "AND FRIENDSHIPS.TIMESTAMP <= '2025-03-31') \n"
          + "AS NUMBER_OF_FRIENDSHIPS \n"
          + ", (SELECT COUNT(POSTS.ID) \n"
          + "FROM POSTS \n"
          + "WHERE POSTS.USERID = USERS.ID) \n"
          + "AS NUMBER_OF_POSTS\t\n"
          + ", SUM(CASE WHEN LIKES.POSTID = POSTS.ID \n"
          + "AND LIKES.TIMESTAMP <= '2025-03-31' \n"
          + "THEN 1 ELSE 0 END) \n"
          + "AS NUMBER_OF_LIKES\n"
          + "FROM USERS\n"
          + "LEFT JOIN POSTS ON USERS.ID = POSTS.USERID\n"
          + "LEFT JOIN LIKES ON LIKES.POSTID = POSTS.ID\n"
          + "GROUP BY USERS.ID\n"
          + "HAVING NUMBER_OF_FRIENDSHIPS > 100 AND NUMBER_OF_LIKES > 100";

  private ReadDao() {}

  public static void readData() {

    try (Connection connection = DataSource.getMySQLConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(READ_DATA)) {
      while (resultSet.next()) {
        logger.info(
            "User - Id: "
                + resultSet.getInt(1)
                + ", Name: "
                + resultSet.getString(2)
                + ", Surname: "
                + resultSet.getString(3)
                + ", Birthday: "
                + resultSet.getDate(4)
                + ", Friendships: "
                + resultSet.getInt(5)
                + ", Posts: "
                + resultSet.getInt(6)
                + ", Likes: "
                + resultSet.getInt(7));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
