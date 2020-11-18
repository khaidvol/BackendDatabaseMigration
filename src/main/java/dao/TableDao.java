package dao;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TableDao {

  private static final Logger logger = Logger.getLogger(TableDao.class);

  public static final String DROP_USERS = "DROP TABLE IF EXISTS USERS;";
  public static final String DROP_FRIENDSHIPS = "DROP TABLE IF EXISTS FRIENDSHIPS;";
  public static final String DROP_POSTS = "DROP TABLE IF EXISTS POSTS;";
  public static final String DROP_LIKES = "DROP TABLE IF EXISTS LIKES;";

  public static final String CREATE_USERS =
      "CREATE TABLE USERS (\n"
          + "\t   ID INT NOT NULL AUTO_INCREMENT\n"
          + "     , NAME VARCHAR(30) NOT NULL\n"
          + "     , SURNAME VARCHAR(30) NOT NULL\n"
          + "     , BIRTHDATE DATE\n"
          + "     , UNIQUE UQ_USERS_1 (ID)\n"
          + "     , PRIMARY KEY (ID)\n"
          + ");";

  public static final String CREATE_FRIENDSHIPS =
      "CREATE TABLE FRIENDSHIPS (\n"
          + "       USERID1 INT NOT NULL\n"
          + "     , USERID2 INT NOT NULL\n"
          + "     , TIMESTAMP DATE\n"
          + "     , CONSTRAINT FK_USERID1 FOREIGN KEY (USERID1) REFERENCES USERS (ID)\n"
          + "\t , CONSTRAINT FK_USERID2 FOREIGN KEY (USERID2) REFERENCES USERS (ID)\n"
          + "\t , CONSTRAINT UQ_FRIENDSHIPS UNIQUE (USERID1, USERID2) \n"
          + ");";

  public static final String CREATE_POSTS =
      "CREATE TABLE POSTS (\n"
          + "       ID INT NOT NULL AUTO_INCREMENT\n"
          + "     , USERID INT NOT NULL\n"
          + "     , TEXT VARCHAR(500) NOT NULL\n"
          + "     , TIMESTAMP DATE\n"
          + "     , PRIMARY KEY (ID)\n"
          + "     , CONSTRAINT FK_POSTS FOREIGN KEY (USERID) REFERENCES USERS (ID)\n"
          + ");";

  public static final String CREATE_LIKES =
      "CREATE TABLE LIKES (\n"
          + "       POSTID INT NOT NULL\n"
          + "     , USERID INT NOT NULL\n"
          + "     , TIMESTAMP DATE\n"
          + "     , CONSTRAINT FK_POSTID FOREIGN KEY (POSTID) REFERENCES POSTS (ID)\n"
          + "\t , CONSTRAINT FK_USERID FOREIGN KEY (USERID) REFERENCES USERS (ID)\n"
          + "\t , CONSTRAINT UQ_LIKES UNIQUE (POSTID, USERID)\n"
          + ");";

  private TableDao() {}

  public static void createAllTables() {
    try (Connection connection = DataSource.getMySQLConnection();
        Statement statement = connection.createStatement()) {
      logger.info("Creating all tables...");
      statement.execute(CREATE_USERS);
      statement.execute(CREATE_FRIENDSHIPS);
      statement.execute(CREATE_POSTS);
      statement.execute(CREATE_LIKES);
      logger.info("SUCCESS");
    } catch (SQLException e) {
      logger.error(e.getMessage());
      e.printStackTrace();
    }
  }

  public static void dropAllTables() {
    try (Connection connection = DataSource.getMySQLConnection();
        Statement statement = connection.createStatement()) {
      logger.info("Dropping all tables...");
      statement.execute(DROP_LIKES);
      statement.execute(DROP_POSTS);
      statement.execute(DROP_FRIENDSHIPS);
      statement.execute(DROP_USERS);
      logger.info("SUCCESS");
    } catch (SQLException e) {
      logger.error(e.getMessage());
      e.printStackTrace();
    }
  }
}
