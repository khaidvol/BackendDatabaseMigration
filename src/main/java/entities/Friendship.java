package entities;

import java.sql.Date;

public class Friendship {

  int userId1;
  int userId2;
  Date timestamp;

  public Friendship() {}

  public Friendship(int userId1, int userId2, Date timestamp) {
    this.userId1 = userId1;
    this.userId2 = userId2;
    this.timestamp = timestamp;
  }

  public int getUserId1() {
    return userId1;
  }

  public void setUserId1(int userId1) {
    this.userId1 = userId1;
  }

  public int getUserId2() {
    return userId2;
  }

  public void setUserId2(int userId2) {
    this.userId2 = userId2;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return "Friendship - userId1: "
        + userId1
        + ", UserId2: "
        + userId2
        + ", Timestamp: "
        + timestamp;
  }
}
