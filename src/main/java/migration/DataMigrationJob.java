package migration;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import dao.DataSource;
import entities.Friendship;
import entities.Like;
import entities.Post;
import entities.User;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class DataMigrationJob {

  private static final Logger logger = Logger.getLogger(DataMigrationJob.class);

  public static final String GET_USER_IDS_SQL = "SELECT USERS.ID FROM USERS";
  public static final String GET_USER_SQL = "SELECT * FROM USERS WHERE USERS.ID = ?";
  public static final String GET_FRIENDSHIPS_SQL =
      "SELECT * FROM FRIENDSHIPS WHERE FRIENDSHIPS.USERID1 = ?";
  public static final String GET_POSTS_SQL = "SELECT * FROM POSTS WHERE POSTS.USERID = ?";
  public static final String GET_LIKES_SQL = "SELECT * FROM LIKES WHERE LIKES.POSTID = ?";

  private DataMigrationJob() {}

  public static void executeMigration() {
    MongoClient mongoClient = DataSource.getMongoClient();
    MongoCollection<Document> collection = DataSource.getMongoCollection();

    List<Integer> userIds = new LinkedList<>();
    List<Document> users = new ArrayList<>();

    try (ClientSession clientSession = mongoClient.startSession();
        Connection connection = DataSource.getMySQLConnection();
        PreparedStatement statementUserIds = connection.prepareStatement(GET_USER_IDS_SQL);
        PreparedStatement statementUser = connection.prepareStatement(GET_USER_SQL);
        PreparedStatement statementFriendships = connection.prepareStatement(GET_FRIENDSHIPS_SQL);
        PreparedStatement statementPosts = connection.prepareStatement(GET_POSTS_SQL);
        PreparedStatement statementLikes = connection.prepareStatement(GET_LIKES_SQL)) {

      // disable auto commit and  get user ids from mysql database
      connection.setAutoCommit(false);
      ResultSet resultSetIds = statementUserIds.executeQuery();

      while (resultSetIds.next()) {
        userIds.add(resultSetIds.getInt(1));
      }
      for (Integer userId : userIds) {
        // read user
        User user = new User();
        statementUser.setInt(1, userId);
        ResultSet resultSetUser = statementUser.executeQuery();
        if (resultSetUser.next()) {
          user.setId(resultSetUser.getInt(1));
          user.setName(resultSetUser.getString(2));
          user.setSurname(resultSetUser.getString(3));
          user.setBirthdate(resultSetUser.getDate(4));
        }

        // read friendships and add them to the user
        statementFriendships.setInt(1, userId);
        ResultSet resultSetFriendships = statementFriendships.executeQuery();
        while (resultSetFriendships.next()) {
          Friendship friendship = new Friendship();
          friendship.setUserId1(resultSetFriendships.getInt(1));
          friendship.setUserId2(resultSetFriendships.getInt(2));
          friendship.setTimestamp(resultSetFriendships.getDate(3));
          user.addFriendship(friendship);
        }

        // read posts and add them to the user
        statementPosts.setInt(1, userId);
        ResultSet resultSetPosts = statementPosts.executeQuery();
        while (resultSetPosts.next()) {
          Post post = new Post();
          post.setId(resultSetPosts.getInt(1));
          post.setUserId(resultSetPosts.getInt(2));
          post.setText(resultSetPosts.getString(3));
          post.setTimestamp(resultSetPosts.getDate(4));

          // read likes and them to the post
          statementLikes.setInt(1, post.getId());
          ResultSet resultSetLikes = statementLikes.executeQuery();

          while (resultSetLikes.next()) {
            Like like = new Like();
            like.setPostId(resultSetLikes.getInt(1));
            like.setUserId(resultSetLikes.getInt(2));
            like.setTimestamp(resultSetLikes.getDate(3));
            post.addLike(like);
          }
          user.addPost(post);
        }

        // list with friendships documents for current user
        List<Document> friendshipsDocs =
            user.getFriendships().stream()
                .map(
                    friendship ->
                        new Document("userId1", friendship.getUserId1())
                            .append("userId2", friendship.getUserId2())
                            .append("timestamp", friendship.getTimestamp()))
                .collect(Collectors.toList());

        // list with posts and likes for current user
        List<Document> postsDocs =
            user.getPosts().stream()
                .map(
                    post ->
                        new Document("id", post.getId())
                            .append("userId", post.getUserId())
                            .append("text", post.getText())
                            .append("timestamp", post.getTimestamp())
                            .append(
                                "likes",
                                post.getLikes().stream()
                                    .map(
                                        like ->
                                            new Document("postId", like.getPostId())
                                                .append("userId", like.getUserId())
                                                .append("timestamp", like.getTimestamp()))
                                    .collect(Collectors.toList())))
                .collect(Collectors.toList());

        // user document with personal info, friendships, posts and likes
        Document userDoc =
            new Document("id", user.getId())
                .append("name", user.getName())
                .append("surname", user.getSurname())
                .append("birthdate", user.getBirthdate())
                .append("friendships", friendshipsDocs)
                .append("posts", postsDocs);

        users.add(userDoc);

        logger.info(user.toString().concat(" - retrieved successfully"));
      }

      // store users to the mongo database
      clientSession.withTransaction(
          () -> {
            collection.insertMany(clientSession, users);
            return "Data inserted.";
          });

      // commit mysql transaction
      connection.commit();

    } catch (SQLException | RuntimeException e) {
      logger.error(e);
    }
    logger.info("Migration finished");
  }
}
