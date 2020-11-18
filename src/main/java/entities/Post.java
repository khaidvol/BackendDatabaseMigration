package entities;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

public class Post {

  private int id;
  private int userId;
  private String text;
  private Date timestamp;
  private List<Like> likes;

  public Post() {
    this.likes = new ArrayList<>();
  }

  public Post(int userId, String text, Date timestamp) {
    this.userId = userId;
    this.text = text;
    this.timestamp = timestamp;
    this.likes = new ArrayList<>();
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getUserId() {
    return userId;
  }

  public void setUserId(int userId) {
    this.userId = userId;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }

  public List<Like> getLikes() {
    return likes;
  }

  public void setLikes(List<Like> likes) {
    this.likes = likes;
  }

  public boolean addLike(Like like) {
    if (likes.contains(like)) return false;
    likes.add(like);
    return true;
  }

  @Override
  public String toString() {
    return "Post - Id: "
        + id
        + ", UserId: "
        + userId
        + ", text: "
        + text
        + ", Timestamp: "
        + timestamp;
  }
}
