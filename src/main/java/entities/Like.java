package entities;

import java.sql.Date;

public class Like {

  int postId;
  int userId;
  Date timestamp;

  public Like() {}

  public Like(int postId, int userId, Date timestamp) {
    this.postId = postId;
    this.userId = userId;
    this.timestamp = timestamp;
  }

  public int getPostId() {
    return postId;
  }

  public void setPostId(int postId) {
    this.postId = postId;
  }

  public int getUserId() {
    return userId;
  }

  public void setUserId(int userId) {
    this.userId = userId;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return "Like - postId1: " + postId + ", UserId: " + userId + ", Timestamp: " + timestamp;
  }
}
