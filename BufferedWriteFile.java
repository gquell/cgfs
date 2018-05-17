/******************************************************************************
 *  BufferedWriteFile.java   
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

/********************************************************************************/
/* Imports */
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/********************************************************************************/
/**
 * Klassenbeschreibung BufferedWriteFile
 */
public class BufferedWriteFile
// implements Serializable
{
  File file = null;
  byte[] buff = null;
  BufferedOutputStream output = null;
  long fsize = 0;
  int bufflen = 0;
  long totalBytesWrite = 0;

  /******************************************************************************/
  static final Logger logger = LoggerFactory.getLogger("BufferedWriteFile");
  /******************************************************************************/
  /**
   *
   * @param fname
   * @param bufflen 
   * @throws java.io.FileNotFoundException 
   */
  public BufferedWriteFile(String fname, int bufflen)
    throws FileNotFoundException
  {
    this.file = new File(fname);
    this.fsize = file.length();
    this.bufflen = bufflen;
    this.buff = new byte[bufflen];
    output = new BufferedOutputStream( new FileOutputStream(this.file) );
  }

  /******************************************************************************/
  /**
  
   * @return 
   */
  public long getTotalBytesWrite()
  {
    return this.totalBytesWrite;
  }

  /******************************************************************************/
  /**
   * @param buff 
   */
  public void writePage(ByteBuffer buff)
  {
    totalBytesWrite = 0;
    this.buff = buff.array();
    try
    {
      output.write(this.buff);
      totalBytesWrite += this.buff.length;
    }
    catch (IOException ex)
    {
      logger.error("writePage :: " + ex.getMessage());
    }
  }

  /******************************************************************************/
  /**
   *
   */
  public void close()
  {
    try
    {
      output.flush();
      output.close();
      file = null;
      buff = null;
      output = null;
      fsize = 0;
      bufflen = 0;
      totalBytesWrite = 0;
    }
    catch (IOException ex)
    {
      logger.error("close :: " + ex.getMessage());
    }
  }

}
/********************************************************************************/
