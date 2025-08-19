package org.apache.spark.util.sketch;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

public class SimpleConcurrentBitSet {

  // as per jdk.internal.util.ArraysSupport::SOFT_MAX_ARRAY_LENGTH
  public static final int MAX_WORDS_LENGTH = Integer.MAX_VALUE - 8;

  private static final int BIT_INDEX_OFFSET_MASK = (1 << 6) - 1;
  private static final VarHandle arrayElement = MethodHandles.arrayElementVarHandle(long[].class);

  private final long bitSize;
  private final LongAdder cardinality;
  private final long[] words;

  public SimpleConcurrentBitSet(long bitSize) {
    assert(bitSize >= 0);

    long longLength = (bitSize >> 6);
    if ((bitSize & BIT_INDEX_OFFSET_MASK) != 0) {
      longLength++;
    }
    assert(longLength <= MAX_WORDS_LENGTH);

    this.bitSize = longLength * Long.SIZE;
    this.cardinality = new LongAdder();
    this.words = new long[(int)longLength];
  }

  public SimpleConcurrentBitSet(
    SimpleConcurrentBitSet that
  ) {
    this.bitSize = that.bitSize;

    long longLength = (bitSize >> 6);
    if ((bitSize & BIT_INDEX_OFFSET_MASK) != 0) {
      longLength++;
    }
    assert(longLength <= MAX_WORDS_LENGTH);
    assert(longLength == that.words.length);
    this.words = Arrays.copyOf(that.words, that.words.length);

    this.cardinality = new LongAdder();
    refreshCardinality();
  }

  long getWordsLength() {
    return words.length;
  }

  long getWord(int longIndex) {
    return (long) arrayElement.getVolatile(words, longIndex);
  }

  public long size() {
    return bitSize;
  }

  public long cardinality() {
    return cardinality.longValue();
  }

  public static long mapToSize(long bitIndex, long bitSize) {
    bitIndex = bitIndex < 0 ? ~bitIndex : bitIndex;
    bitIndex = bitIndex % bitSize;
    return bitIndex;
  }

  public void set(long bitIndex) {
    bitIndex = mapToSize(bitIndex, bitSize);

    int longIndex = (int)(bitIndex >> 6);
    int bitOffset = ((int) bitIndex) & BIT_INDEX_OFFSET_MASK;
    long bitMask = 1L << bitOffset;

    long prev = (long) arrayElement.getAndBitwiseOr(words, longIndex, bitMask);
    if (prev != (prev | bitMask)) {
      cardinality.increment();
    }
  }

  public boolean get(long bitIndex) {
    bitIndex = mapToSize(bitIndex, bitSize);

    int longIndex = (int)(bitIndex >> 6);
    int bitOffset = ((int) bitIndex) & BIT_INDEX_OFFSET_MASK;
    long bitMask = 1L << bitOffset;

    long bitBlock = (long) arrayElement.getVolatile(words, longIndex);
    return (bitBlock & bitMask) != 0;
  }

  // TODO not threadsafe
  public void and(SimpleConcurrentBitSet that) {
    if (this.bitSize != that.bitSize) {
      throw new UnsupportedOperationException(
        "SimpleConcurrentBitSet does not support bitwise operations on instances of different sizes."
      );
    }
    for (int i = 0; i < words.length; i++) {
        arrayElement.getAndBitwiseAnd(
          this.words, i, arrayElement.getVolatile(that.words, i)
        );
    }
    refreshCardinality();
  }

  // TODO not threadsafe
  public void or(SimpleConcurrentBitSet that) {
    if (this.bitSize != that.bitSize) {
      throw new UnsupportedOperationException(
        "SimpleConcurrentBitSet does not support bitwise operations on instances of different sizes."
      );
    }
    for (int i = 0; i < words.length; i++) {
        arrayElement.getAndBitwiseOr(
          this.words, i, arrayElement.getVolatile(that.words, i)
        );
    }
    refreshCardinality();
  }

  public void reset() {
    cardinality.reset();
    for (int i = 0; i < words.length; i++) {
      arrayElement.setVolatile(words, i, 0L);
    }
  }

  private void refreshCardinality() {
    cardinality.reset();
    for (int i = 0; i < words.length; i++) {
      long newValue = (long) arrayElement.getVolatile(this.words, i);
      cardinality.add(Long.bitCount(newValue));
    }
  }

}
