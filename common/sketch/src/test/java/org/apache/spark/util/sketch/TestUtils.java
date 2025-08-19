package org.apache.spark.util.sketch;

public class TestUtils {
  // quick scrambling logic hacked out from java.util.Random
  //   its range is only 48bits (out of the 64bits of a Long value),
  //   but it should be enough for the purposes of this test.
  private static final long multiplier = 0x5DEECE66DL;
  private static final long addend = 0xBL;
  //  private static final long multiplier = 1447878736930374069L;
//  private static final long addend = 69L;
  private static final long mask = (1L << 48) - 1;
  public static long scrambleLong(long value) {
    return (value * multiplier + addend) & mask;
  }
  public static int scrambleInt(int value) {
    return (int) scrambleLong(value);
  }
}
