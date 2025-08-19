/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.sketch;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.LongStream;

public class BloomFilterImplV3
  extends BloomFilter
  implements Serializable {

  protected int numHashFunctions;
  protected List<Function<Object,Long>> hashFunctions;
  protected SimpleConcurrentBitSet bits;
  protected long seed;

  protected BloomFilterImplV3(int numHashFunctions, long numBits, long seed) {
    this.seed = seed;
    this.numHashFunctions = numHashFunctions;
    this.hashFunctions = new ArrayList<>(numHashFunctions);
    this.bits = new SimpleConcurrentBitSet(numBits);

    // TODO generics
    for (int i = 0; i < numHashFunctions; i++) {
      int rank = i + 1;
      hashFunctions.add(i, l -> hash((long)l, rank, (int) seed));
    }
  }

  public static long hash(long value, int rank, int seed) {
    int hi = Murmur3_x86_32.hashLong(value, seed);
    int lo = Murmur3_x86_32.hashLong(value, hi);

    return (long) hi * Integer.MAX_VALUE + (long) lo * rank;
  }

  protected BloomFilterImplV3() {
  }

  @Override
  public double expectedFpp() {
    return Math.pow((double) cardinality() / bitSize(), numHashFunctions);
  }

  @Override
  public long bitSize() {
    return bits.size();
  }

  public boolean putLong(long input) {
    for(int i = 0; i < numHashFunctions; i++) {
      long hash = hashFunctions.get(i).apply(input);
      bits.set(hash);
    }
    return false;
  }

  @Override
  public boolean mightContainLong(long input) {
    for(int i = 0; i < numHashFunctions; i++) {
      long hash = hashFunctions.get(i).apply(input);
      if (!bits.get(hash)) {
        return false;
      }
    }
    return true;
  }

//  public void putAll(LongStream longs) {
//    longs
//      .parallel()
//      .forEach(
//        this::putLong
//      );
//  }
//
//  public LongStream mightContainFilter(LongStream longs) {
//    return longs
//      .parallel()
//      .filter(
//        this::mightContainLong
//      );
//  }

  @Override
  public boolean putBinary(byte[] input) {
    for(int i = 0; i < numHashFunctions; i++) {
      long hash = hashFunctions.get(i).apply(input);
      bits.set(hash);
    }
    return false;
  }

  @Override
  public boolean mightContainBinary(byte[] input) {
    for(int i = 0; i < numHashFunctions; i++) {
      long hash = hashFunctions.get(i).apply(input);
      if (!bits.get(hash)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean putString(String input) {
    return putBinary(Utils.getBytesFromUTF8String(input));
  }

  @Override
  public boolean mightContainString(String input) {
    return mightContainBinary(Utils.getBytesFromUTF8String(input));
  }

  @Override
  public boolean put(Object item) {
    if (item instanceof String str) {
      return putString(str);
    } else if (item instanceof byte[] bytes) {
      return putBinary(bytes);
    } else {
      return putLong(Utils.integralToLong(item));
    }
  }

  @Override
  public boolean mightContain(Object item) {
    if (item instanceof String str) {
      return mightContainString(str);
    } else if (item instanceof byte[] bytes) {
      return mightContainBinary(bytes);
    } else {
      return mightContainLong(Utils.integralToLong(item));
    }
  }

  @Override
  public boolean isCompatible(BloomFilter other) {
    if (other == null) {
      return false;
    }

    if (!(other instanceof BloomFilterImplV3 that)) {
      return false;
    }

    return
      this.getClass() == that.getClass()
        && this.bitSize() == that.bitSize()
        && this.numHashFunctions == that.numHashFunctions
        && this.seed == that.seed;
  }

  @Override
  public BloomFilter mergeInPlace(BloomFilter other) throws IncompatibleMergeException {
    BloomFilterImplV3 that = checkCompatibilityForMerge(other);

    this.bits.or(that.bits);

    return this;
  }

  @Override
  public BloomFilter intersectInPlace(BloomFilter other) throws IncompatibleMergeException {
    BloomFilterImplV3 that = checkCompatibilityForMerge(other);

    this.bits.and(that.bits);

    return this;
  }

  @Override
  public long cardinality() {
    return bits.cardinality();
  }

  protected BloomFilterImplV3 checkCompatibilityForMerge(BloomFilter other)
    throws IncompatibleMergeException {
    // Duplicates the logic of `isCompatible` here to provide better error message.
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null bloom filter");
    }

    if (!(other instanceof BloomFilterImplV3 that)) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filter of class " + other.getClass().getName()
      );
    }

    if (this.bitSize() != that.bitSize()) {
      throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
    }

    if (this.seed != that.seed) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filters with different seeds"
      );
    }

    if (this.numHashFunctions != that.numHashFunctions) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filters with different number of hash functions"
      );
    }

    // TODO
    throw new UnsupportedOperationException();
    //    return that;
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    throw new UnsupportedEncodingException();
//    DataOutputStream dos = new DataOutputStream(out);
//
//    dos.writeInt(Version.V2.getVersionNumber());
//    dos.writeInt(numHashFunctions);
//    dos.writeInt(seed);
//    bits.writeTo(dos);
  }

  private void readFrom0(InputStream in) throws IOException {
    throw new UnsupportedEncodingException();
//    DataInputStream dis = new DataInputStream(in);
//
//    int version = dis.readInt();
//    if (version != Version.V2.getVersionNumber()) {
//      throw new IOException("Unexpected Bloom filter version number (" + version + ")");
//    }
//
//    this.numHashFunctions = dis.readInt();
//    this.seed = dis.readInt();
//    this.bits = BitArray.readFrom(dis);
  }

  public static BloomFilterImplV2 readFrom(InputStream in) throws IOException {
    throw new UnsupportedEncodingException();
//    BloomFilterImplV2 filter = new BloomFilterImplV2();
//    filter.readFrom0(in);
//    return filter;
  }

  @Serial
  private void writeObject(ObjectOutputStream out) throws IOException {
    writeTo(out);
  }

  @Serial
  private void readObject(ObjectInputStream in) throws IOException {
    readFrom0(in);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof BloomFilterImplV3 that)) {
      return false;
    }

    return
      this.getClass() == that.getClass()
        && this.numHashFunctions == that.numHashFunctions
        && this.seed == that.seed
        // TODO: this.bits can be null temporarily, during deserialization,
        //  should we worry about this?
        && this.bits.equals(that.bits);
  }

  @Override
  public int hashCode() {
    return Objects.hash(numHashFunctions, seed, bits);
  }

  @Override
  public String toString() {
    return String.format(
      "%s [%d / %d] %f %%",
      super.toString(),
      cardinality(),
      bitSize(),
      (double) cardinality() / bitSize()
    );
  }


}
