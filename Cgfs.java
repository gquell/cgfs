/** ****************************************************************************
 *  Cgfs.java
 ****************************************************************************** */
/**
 * @author    : Gerhard Que7 13:57:28ll, gquell@skequell.de
 * Erstellt   : 30.06.2017
 * Geaendert  : 1205.2018
 *
 * @Copyright : 2018 by skequell ltd, Krumbach/Germany - www.skequell.com
 * @version : 1.0
 * Description:
 *
 */
/** **************************************************************************** */
package CassandraGridfs;

/** ***************************************************************************** */
/* Imports */
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ***************************************************************************** */
/**
 * Klassenbeschreibung Cgfs
 */
public class Cgfs // implements Serializable
{

  static final Logger logger = LoggerFactory.getLogger("CgfsClass");
  /** ************************************************************************ */
  private String  dbname;
  private Cluster cluster;
  private Session session;
  private String  cpoints = "127.0.0.1"; // Default 
  private long    pgsize = 512000;         // Default 
  private int WRITE_TIMEOUT = 10;

  /** ************************************************************************ */
  /**
   * @param dbname
   * @throws java.lang.Exception
   */
  public Cgfs (String dbname) throws Exception
  {
    if (!openDatabase(dbname, this.cpoints))
    {
      throw new Exception("Cgfs - openDatabase");
    }
  }

  /** ************************************************************************ */
  /**
   *
   * @param dbname
   * @param cpoints
   * @throws java.lang.Exception
   */
  public Cgfs (String dbname, String cpoints) throws Exception
  {
    if (!openDatabase(dbname, cpoints))
    {
      throw new Exception("Cgfs - openDatabase");
    }
  }

  /** ************************************************************************ */
  /**
   * Chunkgroesse
   * @return
   */
  public long getPgsize ()
  {
    return pgsize;
  }

  /** ************************************************************************ */
  /**
   * Chunkgroesse
   * @param pgsize
   */
  public void setPgsize (long pgsize)
  {
    this.pgsize = pgsize;
  }

  /** ************************************************************************ */
  /**
   *
   * @return
   */
  public String getDbname ()
  {
    return this.dbname;
  }

  /** ************************************************************************ */
  /**
   * openDatabase - oeffne vorhandene Datenbank
   * @param dbname
   * @param cpoints
   * @return
   */
  private boolean openDatabase (String dbname, String cpoints)
  {
    this.dbname = dbname;
    this.cpoints = cpoints;

    try
    {
      CgfsCreate cc = new CgfsCreate(this.dbname, this.cpoints);
      cluster = Cluster.builder().addContactPoint(cpoints).build();
      session = cluster.connect(dbname);

      return true;
    }
    catch (Exception ex)
    {
      logger.error("openDatabase(dbname,cpoints) :: " + ex.getMessage());
    }

    return false;
  }

  /** ************************************************************************ */
  /**
   * Schliesse Datenbankverbindungen
   */
  public void close ()
  {
    session.close();
    cluster.close();
    session = null;
    cluster = null;
  }

  /** ************************************************************************ */
  /**
   * Schreibe Datei in Datenbank
   * @param fname
   * @param meta
   * @return
   *         Hashfunktionen sind noch nicht eingebaut
   * @throws java.io.FileNotFoundException
   */
  public UUID writeFile (String fname, Map<String, String> meta)
    throws FileNotFoundException
  {
    String sql1 = "INSERT INTO cgfs_head (h_key,h_fname,h_gsize,h_psize,h_ghash,h_meta)"
      + " values (?,?,?,?,?,?)";
    String sql2 = "INSERT INTO cgfs_data (d_key,d_page,d_hash,d_data) values(?,?,?,?)";
    BufferedReadFile brf;
    ByteBuffer buf;
    brf = new BufferedReadFile(fname, (int) this.pgsize);

    PreparedStatement stmt = session.prepare(sql1);
    BoundStatement bs = new BoundStatement(stmt);
    UUID uuid = UUIDs.random();

    bs.bind().
      setUUID(0, uuid).
      setString(1, fname).
      setLong(2, brf.fsize).
      setLong(3, this.pgsize).
      setString(4, "").
      setMap(5, (Map<String, String>) meta);
    session.execute(bs);

    long pgnr = 1;
    String dbn = this.getDbname();
    stmt = session.prepare(sql2);
    bs = new BoundStatement(stmt);
    do
    {
      buf = brf.readPage();
      bs.bind().
        setUUID(0, uuid).
        setLong(1, pgnr++).
        setString(2, "").
        setBytes(3, buf);
      session.execute(bs);
      try
      {
        Thread.sleep(WRITE_TIMEOUT);
      }
      catch (InterruptedException ex)
      {
      }

    }
    while (buf != null);

    return uuid;
  }

  /** ************************************************************************ */
  /**
   * Lese Datei aus der Datenbank und schreibe sie ins Filesystem
   * @param key
   * @param fname
   * @return
   */
  public boolean readFile (UUID key, String fname)
  {
    String sql1 = "SELECT * FROM cgfs_head where h_key = ?";
    String sql2 = "SELECT * FROM cgfs_data where d_key = ? and d_page = ?";
    PreparedStatement stmt = session.prepare(sql1);
    ResultSet results = session.execute(stmt.bind(key));
    Row row;
    long pgnr = 1;

    try
    {
      if ((row = results.one()) != null)
      {
        BufferedWriteFile bwf = new BufferedWriteFile(fname, (int) row.getLong("h_psize"));
        PreparedStatement stmt2 = session.prepare(sql2);
        ResultSet results2 = null;
        Row row2 = null;
        String dbn = this.getDbname();

        ByteBuffer buff;
        while (true)
        {
          results2 = session.execute(stmt2.bind().
            setUUID(0, key).
            setLong(1, pgnr++));

          if ((row2 = results2.one()) == null)
          {
            break;
          }
          if ((buff = row2.getBytes("d_data")) == null)
          {
            break;
          }
          bwf.writePage(row2.getBytes("d_data"));
        }
        bwf.close();
        return true;
      }
    }
    catch (FileNotFoundException ex)
    {
      logger.error("readFile :: " + ex.getMessage());
    }

    return false;
  }

  /** ************************************************************************ */
  /**
   * Lese Datei aus der Datenbank und schreibe sie ins Filesystem
   * @param key
   * @return
   */
  public Map readFileHead (UUID key)
  {
    HashMap map = new HashMap<String,String>();
    String sql = "SELECT * FROM cgfs_head where h_key = "+ key;
    ResultSet results = session.execute(sql);
    Row row;
    try
    {
      if ((row = results.one()) != null)
      {
        map.put("h_key",row.getUUID("h_key"));
        map.put("h_fname",row.getString("h_fname"));
        map.put("h_gsize",row.getLong("h_gsize"));
        map.put("h_psize",row.getLong("h_psize"));
        map.put("h_meta", row.getMap("h_meta", String.class, String.class));      
        return map;
      }
    }
    catch (Exception ex)
    {
      logger.error("readFileHead :: " + ex.getMessage());
    }
    return null;
  }

  /** ************************************************************************ */
  /** ************************************************************************ */
  /**
   * Lese Datei aus der Datenbank und schreibe sie ins Filesystem
   * @param key
   * @return
   */
  public boolean readFile (UUID key)
  {
    String sql1 = "SELECT * FROM cgfs_head where h_key = ?";
    String sql2 = "SELECT * FROM cgfs_data where d_key = ? and d_page = ?";
    PreparedStatement stmt = session.prepare(sql1);
    ResultSet results = session.execute(stmt.bind(key));
    Row row;
    long pgnr = 1;

    if ((row = results.one()) != null)
    {
      BufferedWriteFile bwf;

      try
      {
        bwf = new BufferedWriteFile(row.getString("h_fname"),
                                    (int) row.getLong("h_psize"));

        PreparedStatement stmt2 = session.prepare(sql2);
        ResultSet results2 = null;
        Row row2 = null;

        do
        {
          results2 = session.execute(stmt2.bind().setUUID(0, key).setLong(1, pgnr++));
          row2 = results2.one();

          if (row2 != null)
          {
            bwf.writePage(row.getBytes("d_data"));
          }
        }
        while (row2 != null);

        bwf.close();

        return true;
      }
      catch (FileNotFoundException ex)
      {
        logger.error("readFile(UUID) :: " + ex.getMessage());
      }
    }

    return false;
  }

  /** ************************************************************************ */
  /**
   * Loeschee Datei aus der Datenbank
   * @param key
   * @return
   */
  public boolean deleteFile (UUID key)
  {
    String sql1 = "DELETE FROM cgfs_data where d_key = ?";
    String sql2 = "DELETE FROM cgfs_head where h_key = ?";
    try
    {
      PreparedStatement stmt1 = session.prepare(sql1);
      PreparedStatement stmt2 = session.prepare(sql2);

      session.execute(stmt1.bind(key));
      session.execute(stmt2.bind(key));
      return true;
    }
    catch (Exception ex)
    {
      logger.error("deleteFile(UUID) :: " + ex.getMessage());
    }

    return false;
  }

  /** ************************************************************************ */
  /**
   * Liste gespeicherte Daten auf
   * @param wmeta Ausgabe mit Metadaten
   * @return
   */
  public long listFiles (boolean wmeta)
  {
    String key;
    String fname;
    long gsize;
    String sql = "SELECT * FROM " + dbname + ".cgfs_head";
    ResultSet results = session.execute(sql);
    Row row;
    long lnr = 0;

    Iterator it = results.iterator();
    System.out.printf("%-40s :: %12s %s\n", "Key", "Size", "Filename");
    while (it.hasNext())
    {
      row = (Row) it.next();
      lnr++;
      key = row.getUUID("h_key").toString();
      fname = row.getString("h_fname");
      gsize = row.getLong("h_gsize");
      System.out.printf("%-40s :: %12d %s\n", key, gsize, fname);

      Map ml = row.getMap("h_meta", String.class, String.class);

      if (wmeta && ml != null && !ml.isEmpty())
      {
        System.out.println("Metadaten <key> <value>:");
        Set ks = ml.keySet();
        for (Object ko : ks)
        {
          System.out.printf(" %-30s :: %s\n", ko, ml.get(ko));
        }
        System.out.println();
      }
    }
    return lnr;
  }

}

/** ***************************************************************************** */
