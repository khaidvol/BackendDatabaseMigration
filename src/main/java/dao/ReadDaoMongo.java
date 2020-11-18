package dao;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadDaoMongo {

  public static final Logger logger = Logger.getLogger(ReadDaoMongo.class);

  private ReadDaoMongo() {}

  public static void readData() {

    MongoCollection<Document> collection = DataSource.getMongoCollection();
    collection.createIndex(Indexes.ascending("name"));
    collection.createIndex(Indexes.ascending("posts"));
    collection.createIndex(Indexes.ascending("likes"));

    List<Document> mongoDBUser =
        collection
            .aggregate(
                Arrays.asList(
                    // Aggregates.match() stage can be used for filtering users
//                    Aggregates.match(Filters.eq("id", "1")),
                    Aggregates.project(
                        Projections.fields(
                            Projections.include("id", "name", "surname", "birthdate"),
                            Projections.computed(
                                "friendsCount", new Document("$size", "$friendships")),
                            Projections.computed("postsCount", new Document("$size", "$posts")),
                            Projections.excludeId()))))
            .map(Document::new)
            .into(new ArrayList<>());

    mongoDBUser.forEach(
        document ->
            logger.info(
                "User - Id: "
                    + document.get("id")
                    + ", Name: "
                    + document.get("name")
                    + ", Surname: "
                    + document.get("surname")
                    + ", Birthday: "
                    + DateTimeFormatter.ofPattern("yyyy-MM-dd")
                        .format(
                            ZonedDateTime.parse(
                                document.get("birthdate").toString(),
                                DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy")))
                    + ", Friendships: "
                    + document.get("friendsCount")
                    + ", Posts: "
                    + document.get("postsCount")));
  }
}
