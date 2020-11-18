package dao;

import entities.Friendship;
import entities.Like;
import entities.Post;
import entities.User;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.List;

public class WriteDao {
  private static final Logger logger = Logger.getLogger(WriteDao.class);

  private static final String INSERT_USER =
      "insert into USERS (NAME, SURNAME, BIRTHDATE) values (?, ?, ?)";
  private static final String INSERT_POST =
      "insert into POSTS (USERID, TEXT, TIMESTAMP) values (?, ?, ?)";
  private static final String INSERT_FRIENDSHIP =
      "insert into FRIENDSHIPS (USERID1, USERID2, TIMESTAMP) values (?, ?, ?)";
  private static final String INSERT_LIKE =
      "insert into LIKES (POSTID, USERID, TIMESTAMP) values (?, ?, ?)";

  private WriteDao() {}

  /** Add Users to the database (one by one) */
  private static void insertUser(User user) {
    try (Connection connection = DataSource.getMySQLConnection();
        PreparedStatement statement =
            connection.prepareStatement(INSERT_USER, Statement.RETURN_GENERATED_KEYS)) {
      statement.setString(1, user.getName());
      statement.setString(2, user.getSurname());
      statement.setDate(3, user.getBirthdate());
      statement.execute();

      ResultSet generatedKeys = statement.getGeneratedKeys();
      if (generatedKeys.next()) {
        user.setId(generatedKeys.getInt(1));
      }

    } catch (SQLException e) {
      logger.error(e.getMessage());
    }
  }

  /** Add Posts to the database (one by one) */
  private static void insertPost(Post post) {
    try (Connection connection = DataSource.getMySQLConnection();
        PreparedStatement statement =
            connection.prepareStatement(INSERT_POST, Statement.RETURN_GENERATED_KEYS)) {
      statement.setInt(1, post.getUserId());
      statement.setString(2, post.getText());
      statement.setDate(3, post.getTimestamp());
      statement.execute();

      ResultSet generatedKeys = statement.getGeneratedKeys();
      if (generatedKeys.next()) {
        post.setId(generatedKeys.getInt(1));
      }

    } catch (SQLException e) {
      logger.error(e.getMessage());
    }
  }

  /** Add Friendships to the database (1000-entries list by one) */
  private static void insertFriendshipList(List<Friendship> friendships) {
    try (Connection connection = DataSource.getMySQLConnection();
        PreparedStatement statement = connection.prepareStatement(INSERT_FRIENDSHIP)) {

      for (Friendship friendship : friendships) {
        statement.setInt(1, friendship.getUserId1());
        statement.setInt(2, friendship.getUserId2());
        statement.setDate(3, friendship.getTimestamp());
        statement.addBatch();
      }
      statement.executeBatch();
    } catch (SQLException e) {
      logger.error(e.getMessage());
    }
  }

  /** Add Likes to the database (1000-entries list by one) */
  private static void insertLikesList(List<Like> likes) {
    try (Connection connection = DataSource.getMySQLConnection();
        PreparedStatement statement = connection.prepareStatement(INSERT_LIKE)) {

      for (Like like : likes) {
        statement.setInt(1, like.getPostId());
        statement.setInt(2, like.getUserId());
        statement.setDate(3, like.getTimestamp());
        statement.addBatch();
      }
      statement.executeBatch();

    } catch (SQLException e) {
      logger.error(e.getMessage());
    }
  }

  /** Add all data to the database */
  public static void populateData() {
    List<User> users = DataGenerator.generateUsersList();
    logger.info("Inserting generated users to the database...");
    users.forEach(WriteDao::insertUser);
    logger.info((long) users.size() + " users inserted successfully");

    List<Post> posts = DataGenerator.generatePostsList();
    logger.info("Inserting generated posts to the database");
    posts.forEach(WriteDao::insertPost);
    logger.info((long) posts.size() + " posts inserted successfully");

    // load 110_000 friendships in 110 batches
    List<Friendship> friendships;
    int startFriendships = 1;
    int endFriendships = 1000;

    for (int i = 0; i < 110; i++) {
      logger.info("Friendships: batch #" + i);
      friendships = DataGenerator.generateFriendshipsList(startFriendships, endFriendships);

      logger.info("Inserting generated friendships to the database");
      insertFriendshipList(friendships);
      logger.info(endFriendships + " friendships inserted successfully");

      startFriendships = endFriendships + 1;
      endFriendships += 1000;
    }

    // load 310_000 likes in 310 batches
    List<Like> likes;
    int startLikes = 1;
    int endLikes = 1000;

    for (int i = 0; i < 310; i++) {
      logger.info("Likes: batch #" + i);
      likes = DataGenerator.generateLikesList(startLikes, endLikes);
      logger.info("Inserting generated likes to the database");
      insertLikesList(likes);
      logger.info(endLikes + " likes inserted successfully");
      startLikes = endLikes + 1;
      endLikes += 1000;
    }
  }
}
