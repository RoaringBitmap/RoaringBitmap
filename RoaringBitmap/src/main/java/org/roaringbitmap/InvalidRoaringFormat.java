package org.roaringbitmap;

import java.io.IOException;

/**
 *
 * Exception thrown when attempting to deserialize a roaring bitmap from
 * an input stream missing a cookie or having other similar anomalies. 
 * Some code may translate it to an IOException
 * for convenience when the cause of the problem can be cleanly interpreted as
 * an IO issue. However, when memory-mapping the file from a ByteBuffer,
 * the exception is used as a RuntimeException.
 *
 */
public class InvalidRoaringFormat extends RuntimeException {

  /**
   * Exception constructor.
   *
   * @param string message
   */
  public InvalidRoaringFormat(String string) {
    super(string);
  }

  /**
   * necessary serial id
   */
  private static final long serialVersionUID = 1L;

  /**
  * Convert the exception to an IOException (convenience function)
  * @return an IOException with a related error message.
  */
  public IOException toIOException() {
    return new IOException(toString());
  }



}
