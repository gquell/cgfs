/** ****************************************************************************
 *  cgfsMain.java
 ****************************************************************************** */
/**
 * @author    : Gerhard Quell, gquell@skequell.de
 * Erstellt   : 30.06.2017
 * Geaendert  : 09.05.2018
 *
 * @Copyright : 2017 by skequell ltd, Krumbach/Germany - www.skequell.com
 * @version : 1.0
 * Description:
 *
 */
/** **************************************************************************** */
package CassandraGridfs;

/** **************************************************************************** */
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ***************************************************************************** */
/**
 * Klassenbeschreibung cgfsMain
 */
public class CgfsMain // implements Serializable
{

  /** ************************************************************************ */
  static final Logger logger = LoggerFactory.getLogger("CgfsMainClass");

  /** ************************************************************************ */
  /**
   * Funktionen
   */
  public enum ToDo
  {
    READ, WRITE, DELETE, LIST, LISTMETA, EXIT, CREATE;
  }

  /** ************************************************************************ */
  public CgfsMain ()
  {

  }

  /** ************************************************************************ */
  /** **** MAIN
   ****
   * @param args the command line arguments
   */
  public static void main (String args[])
  {
    String key = "";
    String fname = "";
    String dbname = "";
    String meta = "";
    String iplist = "127.0.0.1";
    Cgfs cgfs = null;
    ToDo todo = ToDo.EXIT;
    
    
    String dbn = System.getenv("CGFS_DBNAME");
    String ipl = System.getenv("CGFS_IPLIST");
    
    dbname = dbn == null ? "" : dbn;
    iplist = ipl == null ? iplist : ipl;

    CommandLineParser clparser = new PosixParser();
    Options options = new Options();
    options.addOption("db", "database", true, "Datenbank");
    options.addOption("fn", "filename", true, "Filename");
    options.addOption("k", "key", true, "Key");
    options.addOption("m", "meta", true, "Metadata \"{'key':'value',..}\"");
    options.addOption("h", "help", false, "Hilfemenue");
    options.addOption("c", "create", false, "Erstellen");
    options.addOption("r", "read", false, "Lesen");
    options.addOption("w", "write", false, "Schreiben");
    options.addOption("d", "delete", false, "Loeschen") ;
    options.addOption("l", "list", false, "Auflisten aller Files ");
    options.addOption("lm", "listmeta", false, "Auflisten mit Metadaten");

    CommandLine line;

    try
    {
      line = clparser.parse(options, args);

      if (line.hasOption("delete"))
      {
        todo = ToDo.DELETE;
      }
      if (line.hasOption("write"))
      {
        todo = ToDo.WRITE;
      }

      if (line.hasOption("read"))
      {
        todo = ToDo.READ;
      }

      if (line.hasOption("list"))
      {
        todo = ToDo.LIST;
      }
      
      if (line.hasOption("listmeta"))
      {
        todo = ToDo.LISTMETA;
      }

      if (line.hasOption("create"))
      {
        todo = ToDo.CREATE;
      }

      if (line.hasOption("meta"))
      {
        meta = line.getOptionValue("meta");
      }

      if (line.hasOption("key"))
      {
        key = line.getOptionValue("key");
      }

      if (line.hasOption("database"))
      {
        dbname = line.getOptionValue("database");
      }

      if (line.hasOption("filename"))
      {
        fname = line.getOptionValue("filename");
      }

      if (line.hasOption("help"))
      {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("cgfs", options);
        System.exit(2);
      }

      if (dbname.isEmpty())
      {
        System.out.println("Keine Datenbank definiert!");
        logger.error("CREATE :: Keine Datenbank definiert!");
        System.exit(5);
      }
      else
      {
        try
        {
          cgfs = new Cgfs(dbname,iplist);
        }
        catch (Exception ex)
        {
          logger.error("Connection-Error:: " + ex.getMessage());
          System.exit(3);
        }
      }
      
      try
      {
        switch (todo)
        {
          case CREATE:
            new CgfsCreate(dbname,iplist);
            break;

          case WRITE:
            if (fname.isEmpty())
            {
              System.out.println("Keine Datei definiert!");
              logger.error("WRITE :: Keine Datei definiert!");
              System.exit(6);
            }
            /* Falls die Datenbank nicht vorhanden ist, erzeuge eine neue */
            new CgfsCreate(dbname,iplist);
            try
            {
              Map metaMap = null;
              if (meta.isEmpty())
              {
                metaMap = new HashMap();
                metaMap.put("Autor", "Quell,Gerhard");
                metaMap.put("Filename", fname);
              }
              else
              {
                metaMap = parseMeta(meta);
              }

              UUID uuid = cgfs.writeFile(fname, metaMap);
              System.out.println("Datei mit Key : " + uuid.toString() + " geschrieben");
            }
            catch (Exception ex)
            {
              logger.error("WRITE:: " + ex.getMessage());
            }
            break;

          case READ:
            if (key.isEmpty())
            {
              System.out.println("Kein Key definiert");
              logger.error("READ:: Kein Key definiert!");
              System.exit(7);
            }

            try
            {
              UUID ukey = UUID.fromString(key);
              System.out.println("fname: " + fname);
              cgfs.readFile(ukey, fname);
            }
            catch (Exception ex)
            {
              logger.error("READ :: " + ex.getMessage());
              System.exit(8);
            }
            break;

          case DELETE:
            if (key.isEmpty())
            {
              logger.error("READ:: Kein Key definiert!");
              System.exit(7);
            }

            try
            {
              cgfs.deleteFile( UUID.fromString(key));
              System.out.println(key +" wurde gelöscht");
            }            
            catch (Exception ex)
            {
              logger.error("DELETE :: " + ex.getMessage());
              System.exit(8);
            }
            break;
            
          case LIST:
            cgfs.listFiles(false);
            break;

          case LISTMETA:
            cgfs.listFiles(true);
            break;

          case EXIT:
            System.exit(3);
            break;

          default:
            break;
        }
      }
      catch (Exception ex)
      {
        logger.error("Main :: " + Arrays.toString(ex.getStackTrace()));
        System.exit(4);
      }
    }
    catch (org.apache.commons.cli.ParseException ex)
    {
      logger.error("main :: " + Arrays.toString(ex.getStackTrace()));
      System.exit(1);
    }
    catch (Exception ex)
    {
      logger.error("CommandLine-Exception :: " + Arrays.toString(ex.getStackTrace()));
    }
    cgfs.close();
    System.exit(0);
  }

  /** ************************************************************************ */
  /**
   * Wandle Metatext in Hashmap um
   * Format Metatext: "{'key1':'val1','key2':'val2',...}"
   * Achtung: In den Keys und Values sind " und ' verboten - auch nicht als \" oder \'
   * @param inpmap
   * @return
   * @throws Exception
   */
  static HashMap parseMeta (String inpmap)
    throws Exception
  {
    HashMap ret = new HashMap();
    inpmap = inpmap.replace("\'", "\"");
    JSONParser parser = new JSONParser();
    try
    {
      JSONObject o = (JSONObject) parser.parse(inpmap);
      for (Object ob : o.keySet())
      {
        ret.put(ob, o.get(ob));
      }
    }
    catch (ParseException pe)
    {
      logger.error("Metadaten-Fehler an Position: " + pe.getPosition());
      throw new Exception();
    }
    return ret;
  }

}
/** ***************************************************************************** */
