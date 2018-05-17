/******************************************************************************
 *  CgfsCreate.java   
 *******************************************************************************/
/**
 * @author    : Gerhard Quell, gquell@skequell.de
 * Erstellt   : 30.06.2017 
 * Geaendert  : 09.05.2018
 *
 * @Copyright : 2018 by skequell ltd, Krumbach/Germany - www.skequell.com
 * @version   : 1.0
 * Description:
 *
 */
/*******************************************************************************/
package CassandraGridfs;
/*******************************************************************************/
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory; 
/*******************************************************************************/
/* Imports */
/*******************************************************************************/
/**
 * Klassenbeschreibung CgfsCreate
 */
public class CgfsCreate
// implements Serializable
{
  private String dbname;
  private String cpoints             = "127.0.0.1";    // Default  
  private String replicationFactor   = "2";
  private String replicationStrategy = "SimpleStrategy";
  private String durablewrites       = "true";
  /***************************************************************************/
  Cluster cluster;
  Session session;
  /***************************************************************************/
  static final Logger logger = LoggerFactory.getLogger("CgfsCreateClass");
  /***************************************************************************/
  /**
   * Erstelle neue Datenbank
   * @param dbname
   * @throws java.lang.Exception
   */
  public CgfsCreate(String dbname) throws Exception
  {
    this.dbname = dbname;
    if (! createDatabase()) throw new Exception("CgfsCREATE-Error");
  }
  /***************************************************************************/
  /**
   * Erstelle neue Datenbank
   * @param dbname
   * @param cpoints
   * @throws java.lang.Exception
   */
  public CgfsCreate(String dbname, String cpoints) throws Exception
  {
    this.dbname = dbname;
    this.cpoints = cpoints;
    if (! createDatabase()) throw new Exception("CgfsCREATE-Error");
  }
  /***************************************************************************/
  /**
   *  Erstelle neue Datenbank
   * @param dbname
   * @param pgsize
   * @param replicationstrategy
   * @param replicationfactor
   * @param durablewrites
   * @throws java.lang.Exception
   */
  public CgfsCreate(String dbname, 
                    long   pgsize, 
                    String replicationstrategy,
                    String replicationfactor, 
                    String durablewrites) throws Exception
  {
    this.dbname              = dbname;
    this.replicationStrategy = replicationstrategy;
    this.replicationFactor   = replicationfactor;
    this.durablewrites       = durablewrites;
    
    if (!createDatabase())
    {
      throw new Exception("CgfsCREATE-Error");
    }
  }
  /***************************************************************************/
  /**
   *  Erstelle neue Datenbank
   * @param dbname
   * @param cpoints
   * @param pgsize
   * @param replicationstrategy
   * @param replicationfactor
   * @param durablewrites
   * @throws java.lang.Exception
   */
  public CgfsCreate(String dbname, 
                    String cpoints,
                    long   pgsize, 
                    String replicationstrategy,
                    String replicationfactor, 
                    String durablewrites) throws Exception
  {
    this.dbname              = dbname;
    this.cpoints             = cpoints;
    this.replicationStrategy = replicationstrategy;
    this.replicationFactor   = replicationfactor;
    this.durablewrites       = durablewrites;
    
    if (!createDatabase())
    {
      throw new Exception("CgfsCREATE-Error");
    }
  }
  /***************************************************************************/
  /**
   * Erstelle neue Datenbank, wenn sie nicht existiert
   * Replication: SimpleStrategy
   * Rep-Factor : 3
   * DurableWrites : true
   * @return 
   */
  private boolean createDatabase()
  {
    String sql = "CREATE KEYSPACE if not exists "  + this.dbname  
               + " WITH replication = {'class': '" + this.getReplicationStrategy()
               + "','replication_factor': '"       + this.getReplicationFactor()
               + "'} AND durable_writes = "        + this.getDurablewrites() +";";
    try
    {
      cluster = Cluster.builder().addContactPoints(this.cpoints).build();
      session = cluster.connect();
      session.execute(sql);
      session.close();
      session = cluster.connect(dbname);

      session.execute("CREATE TABLE IF NOT EXISTS cgfs_head "
        + "("
        + "  h_key    UUID,"
        + "  h_fname  TEXT,"
        + "  h_gsize  bigint,"
        + "  h_psize  bigint,"
        + "  h_ghash  TEXT,"
        + "  h_meta   map<text, text>,"
        + "  primary key (h_key)"
        + ");"
      );
      session.execute("CREATE TABLE IF NOT EXISTS cgfs_data "
        + "("
        + "  d_key     UUID,"
        + "  d_page    bigint,"
        + "  d_hash    TEXT,"
        + "  d_data    blob,"
        + "  primary key (d_key, d_page)"
        + ");"
      );
      Metadata m = cluster.getMetadata();
      return true;
    }
    catch (Exception ex)
    {
      logger.error("createDatabase :: %s\n" + ex.getMessage());
    }
    finally
    {
      session.close();
    }
    return false;
  }
  /***************************************************************************/
  /**
   * Datenbankname  
   * @return 
  */
  public String getDbname()
  {
    return this.dbname;
  }
  /***************************************************************************/
  /**
   * Setze IP-Liste der Kontaktpunkte
   * @param cpoints 
   */
  public void setCpoints(String cpoints)
  {
    this.cpoints = cpoints;
  }
  /***************************************************************************/
  /**
   * Gebe IP-Liste der Kontaktpunkte zurueck
   * @return 
   */
  public String getCpoints()
  {
    return this.cpoints;
  }
  /***************************************************************************/
  /**
   * ReplicationFactor
   * @return replicationFactor
   */
  public String getReplicationFactor()
  {
    return replicationFactor;
  }
  /***************************************************************************/
  /**
   * ReplicationStrategy
   * @return replicationStrategy
   */
  public String getReplicationStrategy()
  {
    return replicationStrategy;
  }
  /***************************************************************************/
  /**
   * DurableWrites
   * @return durablewrites
   */
  public String getDurablewrites()
  {
    return durablewrites;
  }
  /***************************************************************************/
}
/********************************************************************************/
