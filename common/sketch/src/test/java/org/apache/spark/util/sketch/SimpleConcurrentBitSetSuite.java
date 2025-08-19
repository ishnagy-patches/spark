package org.apache.spark.util.sketch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.stream.IntStream;

public class SimpleConcurrentBitSetSuite {

  static final long K = 1024L;
  static final long M = 1024L * 1024L;
  static final long G = 1024L * 1024L * 1024L;

  @BeforeEach
  public void beforeEach() {
  }

  @Test
  public void testNegativeLength() {
    Assertions.assertThrows(AssertionError.class, () -> {
      new SimpleConcurrentBitSet(-1);
    });
  }

  @Test
  public void testMaxLength() {
    Assertions.assertThrows(AssertionError.class, () -> {
      new SimpleConcurrentBitSet((long) Long.SIZE * SimpleConcurrentBitSet.MAX_WORDS_LENGTH + 1);
    });
  }

  @Test
  public void testWordsLength() {
    SimpleConcurrentBitSet testInstance;

    for (long i = 0; i < 6; i++) {
      testInstance = new SimpleConcurrentBitSet(1L << i);
      Assertions.assertEquals(1L, testInstance.getWordsLength(), "i: " + i);
    }

    for (long i = 6; i < 32; i++) {
      testInstance = new SimpleConcurrentBitSet(1L << i);
      Assertions.assertEquals((1L << i) / 64, testInstance.getWordsLength(), "i: " + i);

      testInstance = new SimpleConcurrentBitSet((1L << i) + 1);
      Assertions.assertEquals(((1L << i) / 64) + 1, testInstance.getWordsLength(), "i: " + i);
    }
  }

  @Test
  public void testBitSetEquivalence() {
    int size = 1 << 24;
    int insertCount = 1 << 12;

    BitSet referenceBitSet = new BitSet(size);
    referenceBitSet.set(size - 1);

    SimpleConcurrentBitSet testInstance = new SimpleConcurrentBitSet(size);
    testInstance.set(size - 1);

    Assertions.assertEquals(
      referenceBitSet.toLongArray().length,
      testInstance.getWordsLength()
    );

    for (int i = 0; i < insertCount; i++) {
      int hash = TestUtils.scrambleInt(i);
      hash = (int) SimpleConcurrentBitSet.mapToSize(hash, testInstance.size());

      referenceBitSet.set(hash);
      testInstance.set(hash);
    }

    Assertions.assertEquals(
      referenceBitSet.toLongArray().length,
      testInstance.getWordsLength()
    );

    Assertions.assertEquals(
      referenceBitSet.cardinality(),
      testInstance.cardinality()
    );

    Assertions.assertEquals(referenceBitSet.cardinality(), testInstance.cardinality());
    for (int i = 0; i < testInstance.getWordsLength(); i++) {
      Assertions.assertEquals(
        Long.toBinaryString(referenceBitSet.toLongArray()[i]),
        Long.toBinaryString(testInstance.getWord(i)),
        "i: " + i
      );
    }

    for (int i = 0; i < insertCount; i++) {
      int hash = TestUtils.scrambleInt(i);
      hash = (int) SimpleConcurrentBitSet.mapToSize(hash, testInstance.size());
      Assertions.assertTrue(testInstance.get(hash));
    }

  }

  @Test
  public void testConcurrentAccess() {
//    int size = 1 << 24;
//    int insertCount = 1 << 12;
    int size = 1 << 4;
    int insertCount = 1 << 2;

    BitSet referenceBitSet = new BitSet(size);
    referenceBitSet.set(size - 1);

    SimpleConcurrentBitSet testInstance = new SimpleConcurrentBitSet(size);
    testInstance.set(size - 1);

    IntStream
      .range(0, insertCount)
      .parallel()
      .map(TestUtils::scrambleInt)
      .forEach(
        testInstance::set
      );
    for (int i = 0; i < insertCount; i++) {
      int hash = TestUtils.scrambleInt(i);
      hash = (int) SimpleConcurrentBitSet.mapToSize(hash, testInstance.size());
      referenceBitSet.set(hash);
    }

    for (int i = 0; i < testInstance.getWordsLength(); i++) {
      Assertions.assertEquals(
        Long.toBinaryString(referenceBitSet.toLongArray()[i]),
        Long.toBinaryString(testInstance.getWord(i)),
        "i: " + i
      );
    }

    boolean setBitsAreTrue = IntStream
      .range(0, insertCount)
      .parallel()
      .map(TestUtils::scrambleInt)
      .allMatch(
        testInstance::get
      );
    Assertions.assertTrue(setBitsAreTrue);
  }

  @Test
  public void testMapToSize() {
    int modulo = (1 << 10);
    int negativeCount = 0;
    int overflowCount = 0;
    for (long i = -5; i < modulo + 5; i++) {
      long reducedValue = SimpleConcurrentBitSet.mapToSize(i, modulo);
      if (reducedValue < 0) {
        negativeCount++;
      }
      if (reducedValue >= modulo) {
        overflowCount++;
      }
    }
    Assertions.assertEquals(0, negativeCount);
    Assertions.assertEquals(0, overflowCount);
  }

  @Test
  public void testReset() {
    int size = 1 << 12;
    int insertCount = 1 << 6;

    SimpleConcurrentBitSet testInstance = new SimpleConcurrentBitSet(size);

    for (int i = 0; i < insertCount; i++) {
      int hash = TestUtils.scrambleInt(i);
      hash = (int) SimpleConcurrentBitSet.mapToSize(hash, testInstance.size());
      testInstance.set(hash);
    }

    testInstance.reset();
    testInstance.set(0);
    testInstance.set(1);
    testInstance.set(2);

    Assertions.assertEquals(3, testInstance.cardinality());

    Assertions.assertEquals(
      Long.toBinaryString(7),
      Long.toBinaryString(testInstance.getWord(0)),
      "i: " + 0
    );
    for (int i = 1; i < testInstance.getWordsLength(); i++) {
      Assertions.assertEquals(
        Long.toBinaryString(0),
        Long.toBinaryString(testInstance.getWord(i)),
        "i: " + i
      );
    }

    Assertions.assertTrue(testInstance.get(0));
    Assertions.assertTrue(testInstance.get(1));
    Assertions.assertTrue(testInstance.get(2));
    for (int i = 3; i < testInstance.size(); i++) {
      Assertions.assertFalse(testInstance.get(i));
    }
  }

  @Test
  public void testAnd() {
    int size = 1 << 24;
    int insertCount = 1 << 23;

    SimpleConcurrentBitSet firstInstance = new SimpleConcurrentBitSet(size);
    SimpleConcurrentBitSet secondInstance = new SimpleConcurrentBitSet(size);

    IntStream
      .range(0, insertCount/2)
      .parallel()
      .map(TestUtils::scrambleInt)
      .forEach(
        firstInstance::set
      );
    IntStream
      .range(insertCount/2, insertCount)
      .parallel()
      .map(TestUtils::scrambleInt)
      .forEach(
        secondInstance::set
      );

    SimpleConcurrentBitSet resultInstance = new SimpleConcurrentBitSet(firstInstance);
    resultInstance.and(secondInstance);

    long expectedCardinality = 0;
    for (int i = 0; i < resultInstance.getWordsLength(); i++) {
      long first = firstInstance.getWord(i);
      long second = secondInstance.getWord(i);
      long expectedResult = first & second;
      long actualResult = resultInstance.getWord(i);
      Assertions.assertEquals(
        Long.toBinaryString(expectedResult),
        Long.toBinaryString(actualResult),
        "i: " + i
      );
      expectedCardinality += Long.bitCount(expectedResult);
    }
    Assertions.assertEquals(
      expectedCardinality,
      resultInstance.cardinality()
    );
  }

  @Test
  public void testOr() {
    int size = 1 << 24;
    int insertCount = 1 << 23;

    SimpleConcurrentBitSet firstInstance = new SimpleConcurrentBitSet(size);
    SimpleConcurrentBitSet secondInstance = new SimpleConcurrentBitSet(size);

    IntStream
      .range(0, insertCount/2)
      .parallel()
      .map(TestUtils::scrambleInt)
      .forEach(
        firstInstance::set
      );
    IntStream
      .range(insertCount/2, insertCount)
      .parallel()
      .map(TestUtils::scrambleInt)
      .forEach(
        secondInstance::set
      );

    SimpleConcurrentBitSet resultInstance = new SimpleConcurrentBitSet(firstInstance);
    resultInstance.or(secondInstance);

    long expectedCardinality = 0;
    for (int i = 0; i < resultInstance.getWordsLength(); i++) {
      long first = firstInstance.getWord(i);
      long second = secondInstance.getWord(i);
      long expectedResult = first | second;
      long actualResult = resultInstance.getWord(i);
      Assertions.assertEquals(
        Long.toBinaryString(expectedResult),
        Long.toBinaryString(actualResult),
        "i: " + i
      );
      expectedCardinality += Long.bitCount(expectedResult);
    }
    Assertions.assertEquals(
      expectedCardinality,
      resultInstance.cardinality()
    );
  }

  @Test
  public void testIncompatibleAnd() {
    SimpleConcurrentBitSet firstInstance = new SimpleConcurrentBitSet(128);
    SimpleConcurrentBitSet secondInstance = new SimpleConcurrentBitSet(256);

    Assertions.assertThrows(
      UnsupportedOperationException.class,
      () -> {
        firstInstance.and(secondInstance);
      }
    );
  }

  @Test
  public void testIncompatibleOr() {
    SimpleConcurrentBitSet firstInstance = new SimpleConcurrentBitSet(128);
    SimpleConcurrentBitSet secondInstance = new SimpleConcurrentBitSet(256);

    Assertions.assertThrows(
      UnsupportedOperationException.class,
      () -> {
        firstInstance.or(secondInstance);
      }
    );
  }

  @Test
  public void testCopy() {
    SimpleConcurrentBitSet firstInstance = new SimpleConcurrentBitSet(128);
    firstInstance.set(0);
    SimpleConcurrentBitSet secondInstance = new SimpleConcurrentBitSet(firstInstance);
    secondInstance.set(0);

    Assertions.assertEquals(firstInstance.size(), secondInstance.size());
    Assertions.assertEquals(firstInstance.cardinality(), secondInstance.cardinality());
    Assertions.assertEquals(firstInstance.getWordsLength(), secondInstance.getWordsLength());
    for (int i = 0; i < firstInstance.getWordsLength(); i++) {
      Assertions.assertEquals(firstInstance.getWord(i), secondInstance.getWord(i));
    }

    // backing arrays are not shared
    firstInstance.set(32);
    secondInstance.set(64);
    Assertions.assertTrue(firstInstance.get(32));
    Assertions.assertFalse(firstInstance.get(64));
    Assertions.assertFalse(secondInstance.get(32));
    Assertions.assertTrue(secondInstance.get(64));
  }

}
