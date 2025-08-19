package org.apache.spark.util.sketch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.spark.util.sketch.BloomFilter.optimalNumOfHashFunctions;

public class BloomFilterImplV2V3CompatSuite {

  @CartesianTest
  public void testHashEquivalence(
    @CartesianTest.Values(longs =
      {
        1_000L,
        10_000L,
        100_000L,
        1_000_000L,
//        10_000_000L,
//        100_000_000L,
        // 400_000_000L
      }
    ) long numItems,
    @CartesianTest.Values(doubles = {0.05, 0.03, 0.01, 0.001}) double expectedFpp,
    @CartesianTest.Values(ints ={BloomFilterImpl.DEFAULT_SEED, 1, 127}) int deterministicSeed
  ) {
    long optimalNumOfBits = BloomFilter.optimalNumOfBits(numItems, expectedFpp);
    int numHashFunctions = optimalNumOfHashFunctions(numItems, optimalNumOfBits);

    BloomFilterImplV2 bfv2 = new BloomFilterImplV2(
      numHashFunctions,
      optimalNumOfBits,
      deterministicSeed
    );

    BloomFilterImplV3 bfv3 = new BloomFilterImplV3(
      numHashFunctions,
      optimalNumOfBits,
      deterministicSeed
    );

    Map<Long, Long> discrepancies = new TreeMap<>();
    for(long i = 0; i < numItems; i++) {
      bfv2.putLong(i);
      for (int h = 0; h < numHashFunctions; h++){
        long bitIndex = BloomFilterImplV3.hash(i, h + 1, deterministicSeed);
        bitIndex = SimpleConcurrentBitSet.mapToSize(bitIndex, bfv3.bitSize());
        if (!bfv2.bits.get(bitIndex)) {
          discrepancies.put(i, bitIndex);
        }
      }
    }
    Assertions.assertEquals(Collections.emptyMap(), discrepancies);
  }
}
