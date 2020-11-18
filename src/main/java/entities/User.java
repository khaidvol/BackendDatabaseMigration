package entities;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

public class User {
  private int id;
  private String name;
  private String surname;
  private Date birthdate;
  private List<Friendship> friendships;
  private List<Post> posts;

  public User() {
    this.friendships = new ArrayList<>();
    this.posts = new ArrayList<>();
  }

  public User(String name, String surname, Date birthdate) {
    this.name = name;
    this.surname = surname;
    this.birthdate = birthdate;
    this.friendships = new ArrayList<>();
    this.posts = new ArrayList<>();
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSurname() {
    return surname;
  }

  public void setSurname(String surname) {
    this.surname = surname;
  }

  public Date getBirthdate() {
    return birthdate;
  }

  public void setBirthdate(Date birthdate) {
    this.birthdate = birthdate;
  }

  public List<Friendship> getFriendships() {
    return friendships;
  }

  public void setFriendships(List<Friendship> friendships) {
    this.friendships = friendships;
  }

  public boolean addFriendship(Friendship friend) {
    if (friendships.contains(friend)) return false;
    friendships.add(friend);
    return true;
  }

  public List<Post> getPosts() {
    return posts;
  }

  public void setPosts(List<Post> posts) {
    this.posts = posts;
  }

  public boolean addPost(Post post) {
    if (posts.contains(post)) return false;
    posts.add(post);
    return true;
  }

  @Override
  public String toString() {
    return "User - Id: "
        + id
        + ", Name: "
        + name
        + ", Surname: "
        + surname
        + ", Birthday: "
        + birthdate;
  }
}
