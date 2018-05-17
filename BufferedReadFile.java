  /******************************************************************************
   *  BufferdReadFile.java
   *******************************************************************************/

  /**
   * @author    : Gerhard Quell, gquell@skequell.de
   * Erstellt   : 30.06.2017
   * Geaendert  : 10.05.2018
   *
   * @Copyright : 2018 by skequell ltd, Krumbach/Germany - www.skequell.com
   * @version   : 1.0
   * Description:
   *
   */

  /*******************************************************************************/
  package CassandraGridfs;

  import java.io.BufferedInputStream;
  import java.io.File;
  import java.io.FileInputStream;
  import java.io.FileNotFoundException;
  import java.io.IOException;
  import java.io.InputStream;
  import java.nio.ByteBuffer;
  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;


  /********************************************************************************/
  /**
   * Klassenbeschreibung BufferdReadFile
   */
  public class BufferedReadFile// implements Serializable

  {
    /******************************************************************************/
    static final Logger logger = LoggerFactory.getLogger("BufferedReadFile");
    File file = null;
    byte buff[] = null;
    InputStream input = null;
    long fsize = 0;
    int bufflen = 0;
    long totalBytesRead = 0;

    /******************************************************************************/
    /**
     *
     * @param fname
     * @param bufflen
     * @throws java.io.FileNotFoundException
    */
    public BufferedReadFile(String fname, int bufflen)
                       throws FileNotFoundException
    {
      this.file = new File(fname);
      this.fsize = file.length();
      this.bufflen = bufflen;
      this.buff = new byte[bufflen];
      input = new BufferedInputStream(new FileInputStream(file));
    }

    /******************************************************************************/
    /**

     * @return
    */
    public long getTotalBytesRead()
    {
      return this.totalBytesRead;
    }

    /******************************************************************************/
    /**
     * Lese Seite aus dem File in den Buffer
     * @return
    */
    public ByteBuffer readPage()
    {
      int size;

      try
      {
        size = input.read(buff);

        if (size < 0)
        {
          return null;
        }

        totalBytesRead += size;

        if (size < this.bufflen)
        {
          byte buff2[] = new byte[size];
          System.arraycopy(buff, 0, buff2, 0, size);
          return ByteBuffer.wrap(buff2);
        }
        else
        {
          return ByteBuffer.wrap(buff);
        }
      }
      catch (IOException ex)
      {
        logger.error("readPage :: " + ex.getMessage());
      }

      return null;
    }

    /******************************************************************************/
    /**
     *
    */
    public void close()
    {
      file = null;
      buff = null;
      input = null;
      fsize = 0;
      bufflen = 0;
      totalBytesRead = 0;
    }
  }
  /********************************************************************************/
