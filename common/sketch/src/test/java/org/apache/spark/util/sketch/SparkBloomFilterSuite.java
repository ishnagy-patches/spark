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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

import org.apache.spark.util.sketch.BloomFilter.Version;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.LongStream;

@EnabledIfEnvironmentVariable(
  named = "SPARK_TEST_SPARK_BLOOM_FILTER_SUITE_ENABLED", matches = "true")
public class SparkBloomFilterSuite {

  // the implemented fpp limit is only approximating the hard boundary,
  // so we'll need some tolerance for the assertion
  static final double FPP_ERROR_TOLERANCE = 0.06;

  static final long K = 1024L;
  static final long M = 1024L * 1024L;
  static final long G = 1024L * 1024L * 1024L;

  static final long REQUIRED_HEAP_UPPER_BOUND_IN_BYTES = 8 * G;

  static final Map<String, Long> longValueByLabel = new HashMap<>();
  static {
    longValueByLabel.put("1K", 1 * K);
    longValueByLabel.put("10K", 10 * K);
    longValueByLabel.put("100K", 100 * K);
    longValueByLabel.put("1M", 1 * M);
    longValueByLabel.put("10M", 10 * M);
    longValueByLabel.put("100M", 100 * M);
    longValueByLabel.put("400M", 400 * M);
    longValueByLabel.put("1G", 1 * G);
    longValueByLabel.put("5G", 5 * G);
  }

  private static Instant START;
  private static boolean strict;

  private Instant start;
  private final Map<String,PrintStream> testOutMap = new ConcurrentHashMap<>();

  @BeforeAll
  public static void beforeAll() {
    START = Instant.now();

    String testClassName = SparkBloomFilterSuite.class.getName();
    strict = Boolean.parseBoolean(System.getProperty(testClassName+ ".strict", "true"));
  }

  @AfterAll
  public static void afterAll() {
    Duration duration = Duration.between(START, Instant.now());
  }

  @BeforeEach
  public void beforeEach(
    TestInfo testInfo
  ) throws Exception {
    start = Instant.now();

    String testName = testInfo.getDisplayName();

    String testClassName = SparkBloomFilterSuite.class.getName();
    String logDir = System.getProperty(testClassName+ ".logDir", "./target/tmp");
    Path logDirPath = Path.of(logDir);
    Files.createDirectories(logDirPath);
    Path testLogPath = Path.of(logDir,testName + ".log");
    Files.deleteIfExists(testLogPath);

    PrintStream testOut = new PrintStream(Files.newOutputStream(testLogPath));
    testOutMap.put(testName, testOut);

    testOut.println("testName: " + testName);
  }

  @AfterEach
  public void afterEach(TestInfo testInfo) {
    Duration duration = Duration.between(start, Instant.now());

    String testName = testInfo.getDisplayName();
    PrintStream testOut = testOutMap.get(testName);
    testOut.println("duration: " + duration );
    testOut.close();
  }

  private long longFromLabel(String label) {
    Long result = longValueByLabel.get(label);
    if (result == null) {
      throw new IllegalArgumentException("Unknown long label: '" + label + "'");
    }
    return result;
  }

  /**
   * This test, in N number of iterations, inserts N even numbers (2*i) int,
   * and leaves out N odd numbers (2*i+1) from the tested BloomFilter instance.
   *
   * It checks the 100% accuracy of mightContain=true on all of the even items,
   * and measures the mightContain=true (false positive) rate on the not-inserted odd numbers.
   *
   * @param numItemsString string key to look up a long value from the longValueByLabel map.
   *                       the resulting long value will be used as an insertion count in the test.
   * @param expectedFpp the expected fpp rate of the tested BloomFilter instance
   * @param deterministicSeed the deterministic seed to use to initialize
   *                          the primary BloomFilter instance.
   */
  @CartesianTest(name = "evenOdd.t{index}_{0}_f{1}_s{2}_{3}")
  public void testAccuracyEvenOdd(
    @Values(doubles = {0.03}) double expectedFpp,
    @Values(ints ={BloomFilterImpl.DEFAULT_SEED}) int deterministicSeed,
    @Values(strings =
      {
        "1K",
        "10K",
        "100K",
        "1M",
        "10M",
        "100M",
        "400M",
        "1G",
        "5G"
      }
    ) String numItemsString,
    @Values(strings = {"V3", "V2"}) String versionString,
    TestInfo testInfo
  ) throws Exception
  {
    String testName = testInfo.getDisplayName();
    PrintStream testOut = testOutMap.get(testName);

    Version version = Version.valueOf(versionString);
    long numItems = longFromLabel(numItemsString);
    if (version == Version.V2) {
      Assumptions.assumeTrue(numItems <= 1 * G);
    }

    long optimalNumOfBits = BloomFilter.optimalNumOfBits(numItems, expectedFpp);
    testOut.printf(
      "optimal   bitArray: %d (%d MB)\n",
      optimalNumOfBits,
      optimalNumOfBits / Byte.SIZE / 1024 / 1024
    );
    Assumptions.assumeTrue(
      optimalNumOfBits / Byte.SIZE < REQUIRED_HEAP_UPPER_BOUND_IN_BYTES,
      "this testcase would require allocating more than 4GB of heap mem ("
        + optimalNumOfBits
        + " bits)"
    );

    BloomFilter bloomFilter =
      BloomFilter.create(
        version,
        numItems,
        optimalNumOfBits,
        deterministicSeed
      );

    testOut.printf(
      "allocated bitArray: %d (%d MB)\n",
      bloomFilter.bitSize(),
      bloomFilter.bitSize() / Byte.SIZE / 1024 / 1024
    );

    if (bloomFilter instanceof BloomFilterImplV3 bfv3) {
      LongStream.range(0, numItems)
        .parallel()
        .forEach(l -> {
            bfv3.putLong(2 * l);
          }
        );
    } else {
      for (long l = 0; l < numItems; l++) {
        bloomFilter.putLong(2 * l);
      }
    }

    testOut.printf("bitCount: %d\nsaturation: %f\n",
      bloomFilter.cardinality(),
      (double) bloomFilter.cardinality() / bloomFilter.bitSize()
    );

    LongAdder mightContainEven = new LongAdder();
    LongAdder mightContainOdd = new LongAdder();

    LongStream inputStream = LongStream.range(0, numItems).parallel();
    inputStream.forEach(
      l -> {
        long even = 2 * l;
        if (bloomFilter.mightContainLong(even)) {
          mightContainEven.increment();
        }

        long odd = 2 * l + 1;
        if (bloomFilter.mightContainLong(odd)) {
          mightContainOdd.increment();
        }
      }
    );

    Assertions.assertEquals(
      numItems, mightContainEven.longValue(),
      "mightContainLong must return true for all inserted numbers"
    );

    double actualFpp = mightContainOdd.doubleValue() / numItems;
    double acceptableFpp = expectedFpp * (1 + FPP_ERROR_TOLERANCE);
    double relFppError = actualFpp / expectedFpp - 1;

    testOut.printf("expectedFpp:   %f %%\n", 100 * expectedFpp);
    testOut.printf("actualFpp:     %f %%\n", 100 * actualFpp);
    testOut.printf("relFppError:   %f %%\n", 100 * relFppError);

    if (!strict) {
      Assumptions.assumeTrue(
        actualFpp <= acceptableFpp,
        String.format(
          "acceptableFpp(%f %%) < actualFpp (%f %%) // rel error: %f %%",
          100 * acceptableFpp,
          100 * actualFpp,
          100 * relFppError
        )
      );
    } else {
      Assertions.assertTrue(
        actualFpp <= acceptableFpp,
        String.format(
          "acceptableFpp(%f %%) < actualFpp (%f %%) // rel error: %f %%",
          100 * acceptableFpp,
          100 * actualFpp,
          100 * relFppError
        )
      );
    }
  }

  /**
   * This test inserts N pseudorandomly generated numbers in 2N number of iterations in two
   * differently seeded (theoretically independent) BloomFilter instances. All the random
   * numbers generated in an even-iteration will be inserted into both filters, all the
   * random numbers generated in an odd-iteration will be left out from both.
   *
   * The test checks the 100% accuracy of 'mightContain=true' for all the items inserted
   * in an even-loop. It counts the false positives as the number of odd-loop items for
   * which the primary filter reports 'mightContain=true', but secondary reports
   * 'mightContain=false'. Since we inserted the same elements into both instances,
   * and the secondary reports non-insertion, the 'mightContain=true' from the primary
   * can only be a false positive.
   *
   * @param numItemsString string key to look up a long value from the longValueByLabel map.
   *                       the resulting long value will be used as an insertion count in the test.
   * @param expectedFpp the expected fpp rate of the tested BloomFilter instance
   * @param deterministicSeed the deterministic seed to use to initialize
   *                          the primary BloomFilter instance. (The secondary will be
   *                          initialized with the constant seed of 0xCAFEBABE)
   */
  @CartesianTest(name = "randomDistribution.t{index}_{0}_f{1}_s{2}_{3}")
  public void testAccuracyRandomDistribution(
    @Values(doubles = {0.03}) double expectedFpp,
    @Values(ints ={BloomFilterImpl.DEFAULT_SEED}) int deterministicSeed,
    @Values(strings =
      {
        "1K",
        "10K",
        "100K",
        "1M",
        "10M",
        "100M",
        "400M",
        "1G",
        "5G"
      }
    ) String numItemsString,
    @Values(strings = {"V3", "V2"}) String versionString,
    TestInfo testInfo
  ) throws Exception
  {
    String testName = testInfo.getDisplayName();
    PrintStream testOut = testOutMap.get(testName);

    Version version = Version.valueOf(versionString);
    long numItems = longFromLabel(numItemsString);
    if (version == Version.V2) {
      Assumptions.assumeTrue(numItems <= 1 * G);
    }

    long optimalNumOfBits = BloomFilter.optimalNumOfBits(numItems, expectedFpp);
    testOut.printf(
      "optimal   bitArray: %d (%d MB)\n",
      optimalNumOfBits,
      optimalNumOfBits / Byte.SIZE / 1024 / 1024
    );
    Assumptions.assumeTrue(
      2 * optimalNumOfBits / Byte.SIZE < REQUIRED_HEAP_UPPER_BOUND_IN_BYTES,
      "this testcase would require allocating more than 8GB of heap mem (2x "
        + optimalNumOfBits
        + " bits)"
    );


    BloomFilter bloomFilterPrimary =
      BloomFilter.create(
        version,
        numItems,
        optimalNumOfBits,
        deterministicSeed
      );

    // V1 ignores custom seed values, so the control filter must be at least V2
    BloomFilter bloomFilterSecondary =
      BloomFilter.create(
        version == Version.V1? Version.V2 : version,
        numItems,
        optimalNumOfBits,
        0xCAFEBABE
      );

    testOut.printf(
      "allocated bitArray: %d (%d MB)\n",
      bloomFilterPrimary.bitSize(),
      bloomFilterPrimary.bitSize() / Byte.SIZE / 1024 / 1024
    );

    if (
      bloomFilterPrimary instanceof BloomFilterImplV3 concurrentFilterPrimary
      && bloomFilterSecondary instanceof BloomFilterImplV3 concurrentFilterSecondary
    ) {
      LongStream.range(0, numItems)
        .parallel()
        .forEach(l -> {
            long scrambledEvenValue = TestUtils.scrambleLong(2 * l);
            concurrentFilterPrimary.putLong(scrambledEvenValue);
            concurrentFilterSecondary.putLong(scrambledEvenValue);
          }
        );
    } else {
      for (long l = 0; l < numItems; l++) {
        long scrambledEvenValue = TestUtils.scrambleLong(2 * l);
        bloomFilterPrimary.putLong(scrambledEvenValue);
        bloomFilterSecondary.putLong(scrambledEvenValue);
      }
    }

    testOut.printf("bitCount: %d\nsaturation: %f\n",
      bloomFilterPrimary.cardinality(),
      (double) bloomFilterPrimary.cardinality() / bloomFilterPrimary.bitSize()
    );

    LongAdder mightContainScrambledEven = new LongAdder();
    LongAdder mightContainScrambledOdd = new LongAdder();
    LongAdder confirmedAsNotInserted = new LongAdder();

    LongStream inputStream = LongStream.range(0, numItems).parallel();
    inputStream.forEach(
      l -> {

        long scrambledEvenValue = TestUtils.scrambleLong(2 * l);
        long scrambledOddValue = TestUtils.scrambleLong(2 * l + 1);

        // EVEN
        if (bloomFilterPrimary.mightContainLong(scrambledEvenValue)) {
          mightContainScrambledEven.increment();
        }

        // ODD
        // for fpp estimation, only consider the odd indexes
        // (to avoid querying the secondary with elements known to be inserted)

        // since here we avoided all the even indexes,
        // most of these secondary queries will return false
        if (!bloomFilterSecondary.mightContainLong(scrambledOddValue)) {
          // from the odd indexes, we consider only those items
          // where the secondary confirms the non-insertion

          // anything on which the primary and the secondary
          // disagrees here is a false positive
          if (bloomFilterPrimary.mightContainLong(scrambledOddValue)) {
            mightContainScrambledOdd.increment();
          }
          // count the total number of considered items for a baseline
          confirmedAsNotInserted.increment();
        }

      }
    );

    Assertions.assertEquals(
      numItems, mightContainScrambledEven.longValue(),
      "mightContainLong must return true for all inserted numbers"
    );

    double actualFpp =
      mightContainScrambledOdd.doubleValue() / confirmedAsNotInserted.doubleValue();

    double acceptableFpp = (1 + FPP_ERROR_TOLERANCE) * expectedFpp;
    double relFppError = actualFpp / expectedFpp - 1;

    testOut.printf("numItems:                 %10d\n", numItems);
    testOut.printf("mightContainScrambledOdd: %10d\n", mightContainScrambledOdd.longValue());
    testOut.printf("confirmedAsNotInserted:   %10d\n", confirmedAsNotInserted.longValue());
    testOut.printf("expectedFpp:       %f %%\n", 100 * expectedFpp);
    testOut.printf("actualFpp:         %f %%\n", 100 * actualFpp);
    testOut.printf("relFppError:       %f %%\n", 100 * relFppError);

    if (!strict) {
      Assumptions.assumeTrue(
        actualFpp <= acceptableFpp,
        String.format(
          "acceptableFpp(%f %%) < actualFpp (%f %%) // rel error: %f %%",
          100 * acceptableFpp,
          100 * actualFpp,
          100 * relFppError
        )
      );
    } else {
      Assertions.assertTrue(
        actualFpp <= acceptableFpp,
        String.format(
          "acceptableFpp(%f %%) < actualFpp (%f %%) // rel error: %f %%",
          100 * acceptableFpp,
          100 * actualFpp,
          100 * relFppError
        )
      );
    }
  }



}
