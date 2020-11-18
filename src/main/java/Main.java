import dao.ReadDao;
import dao.ReadDaoMongo;
import dao.TableDao;
import dao.WriteDao;
import migration.DataMigrationJob;

public class Main {
  public static void main(String[] args) {

//    TableDao.dropAllTables();
//    TableDao.createAllTables();
//    WriteDao.populateData();
//    ReadDao.readData();
//
    DataMigrationJob.executeMigration();
    ReadDaoMongo.readData();
  }
}
