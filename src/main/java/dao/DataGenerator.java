package dao;

import com.jramoyo.io.IndexedFileReader;
import entities.Friendship;
import entities.Like;
import entities.Post;
import entities.User;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataGenerator {

  public static final String SUCCESS = "SUCCESS";
  private static final Logger logger = Logger.getLogger(DataGenerator.class);

  private DataGenerator() {}

  public static List<User> generateUsersList() {
    logger.info("Generating list of Users... ");

    List<User> users = new ArrayList<>();

    try (BufferedReader bufferedReader =
        new BufferedReader(new FileReader("src/main/resources/users.txt"))) {

      for (int i = 0; i < 1000; i++) {
        String[] user = bufferedReader.readLine().split(", ");
        users.add(new User(user[0], user[1], Date.valueOf(user[2])));
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    logger.info(SUCCESS);
    return users;
  }

  public static List<Post> generatePostsList() {
    logger.info("Generating list of Posts... ");

    List<Post> posts = new ArrayList<>();

    try (BufferedReader bufferedReader =
        new BufferedReader(new FileReader("src/main/resources/posts.txt"))) {

      for (int i = 0; i < 1200; i++) {
        String[] post = bufferedReader.readLine().split("; ");
        posts.add(new Post(Integer.parseInt(post[0]), post[1], Date.valueOf(post[2])));
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    logger.info(SUCCESS);
    return posts;
  }

  public static List<Friendship> generateFriendshipsList(int start, int end) {
    logger.info("Generating list of Friendships... ");

    List<Friendship> friendships = new ArrayList<>();
    File file = new File("src/main/resources/friendships.txt");

    try (IndexedFileReader indexedFileReader = new IndexedFileReader(file)) {
      Map<Integer, String> lines = indexedFileReader.readLines(start, end);
      for (Map.Entry<Integer, String> line : lines.entrySet()) {
        String[] friendship = line.getValue().split("; ");
        friendships.add(
            new Friendship(
                Integer.parseInt(friendship[0]),
                Integer.parseInt(friendship[1]),
                Date.valueOf(friendship[2])));
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    logger.info(SUCCESS);
    return friendships;
  }

  public static List<Like> generateLikesList(int start, int end) {
    logger.info("Generating list of Likes... ");

    List<Like> likes = new ArrayList<>();
    File file = new File("src/main/resources/likes.txt");

    try (IndexedFileReader indexedFileReader = new IndexedFileReader(file)) {
      Map<Integer, String> lines = indexedFileReader.readLines(start, end);
      for (Map.Entry<Integer, String> line : lines.entrySet()) {
        String[] like = line.getValue().split("; ");
        likes.add(
            new Like(Integer.parseInt(like[0]), Integer.parseInt(like[1]), Date.valueOf(like[2])));
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    logger.info(SUCCESS);
    return likes;
  }
}
