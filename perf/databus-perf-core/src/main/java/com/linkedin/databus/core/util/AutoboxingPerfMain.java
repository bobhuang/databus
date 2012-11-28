package com.linkedin.databus.core.util;

import java.util.Formatter;
import java.util.Random;

/**
 * Tests the performance of autoboxing
 * @author cbotev
 *
 */
public class AutoboxingPerfMain
{

  private final static int MAX_VALUES = 100000;
  private final static int ITER_NUM = 2000;

  private static final short MIN_CACHED_VALUE = Short.MIN_VALUE;
  private static final short MAX_CACHED_VALUE = Short.MAX_VALUE;
  private static final Short[] SHORT_CACHE = new Short[MAX_CACHED_VALUE - MIN_CACHED_VALUE + 1];
  private static long cacheMem = 0;

  static
  {
    long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    for (int i = MIN_CACHED_VALUE ; i <= MAX_CACHED_VALUE; ++i)
    {
      SHORT_CACHE[i - MIN_CACHED_VALUE] = (short)i;
    }
    long endMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    cacheMem = endMem - startMem;
  }

  public void testIntAutoboxing() throws Exception
  {
    //init
    Random rng = new Random();
    int[] values = new int[MAX_VALUES];
    for (int i = 0; i < MAX_VALUES; ++i) values[i] = rng.nextInt();

    int[] primitiveCopy = new int[MAX_VALUES];
    Integer[] objCopyAuto = new Integer[MAX_VALUES];
    Integer[] objCopyNew = new Integer[MAX_VALUES];

    long primitiveElapsed = 0;
    long autoElapsed = 0;
    long newElapsed = 0;
    for (int iter = 0; iter < ITER_NUM; ++iter)
    {
      long ts1 = System.nanoTime();

      for (int i = 0; i < MAX_VALUES; ++i) primitiveCopy[i] = values[i];

      long ts2 = System.nanoTime();

      for (int i = 0; i < MAX_VALUES; ++i) objCopyAuto[i] = values[i];

      long ts3 = System.nanoTime();

      for (int i = 0; i < MAX_VALUES; ++i) objCopyNew[i] = Integer.valueOf(values[i]);

      long ts4 = System.nanoTime();

      primitiveElapsed += ts2 - ts1;
      autoElapsed += ts3 - ts2;
      newElapsed += ts4 - ts3;
    }

    printTime("int primitive", primitiveElapsed);
    printTime("int auto", autoElapsed);
    printTime("int new", newElapsed);
  }

  public void testShortAutoboxing() throws Exception
  {
    System.out.println("Short size:" + cacheMem + " per-object:" + (1.0 * cacheMem / (MAX_CACHED_VALUE - MIN_CACHED_VALUE + 1)));

    //init
    Random rng = new Random();
    short[] values = new short[MAX_VALUES];
    for (int i = 0; i < MAX_VALUES; ++i) values[i] = (short)rng.nextInt();

    short[] niceValues = new short[MAX_VALUES];
    for (int i = 0; i < MAX_VALUES; ++i) niceValues[i] = (short)((rng.nextInt() - MIN_CACHED_VALUE) % MAX_CACHED_VALUE) ;

    short[] primitiveCopy = new short[MAX_VALUES];
    Short[] objCopyAuto = new Short[MAX_VALUES];
    Short[] objCopyNew = new Short[MAX_VALUES];
    Short[] objCopyCache = new Short[MAX_VALUES];

    long primitiveElapsed = 0;
    long autoElapsed = 0;
    long newElapsed = 0;
    long cacheRandElapsed = 0;
    long cacheHitElapsed = 0;
    for (int iter = 0; iter < ITER_NUM; ++iter)
    {
      long ts1 = System.nanoTime();

      for (int i = 0; i < MAX_VALUES; ++i) primitiveCopy[i] = values[i];

      long ts2 = System.nanoTime();

      for (int i = 0; i < MAX_VALUES; ++i) objCopyAuto[i] = values[i];

      long ts3 = System.nanoTime();

      for (int i = 0; i < MAX_VALUES; ++i) objCopyNew[i] = Short.valueOf(values[i]);

      long ts4 = System.nanoTime();

      for (int i = 0; i < MAX_VALUES; ++i)
      {
        objCopyCache[i] = SHORT_CACHE[values[i] - MIN_CACHED_VALUE];
      }

      long ts5 = System.nanoTime();

      for (int i = 0; i < MAX_VALUES; ++i)
      {
        objCopyCache[i] = SHORT_CACHE[niceValues[i] - MIN_CACHED_VALUE];
      }

      long ts6 = System.nanoTime();

      primitiveElapsed += ts2 - ts1;
      autoElapsed += ts3 - ts2;
      newElapsed += ts4 - ts3;
      cacheRandElapsed += ts5 - ts4;
      cacheHitElapsed += ts6 - ts5;
    }

    printTime("short primitive", primitiveElapsed);
    printTime("short auto", autoElapsed);
    printTime("short new", newElapsed);
    printTime("short cached (rnd)", cacheRandElapsed);
    printTime("short cached (hit)", cacheHitElapsed);
  }

  public void testByteAutoboxing() throws Exception
  {
    //init
    Random rng = new Random();
    byte[] values = new byte[MAX_VALUES];
    for (int i = 0; i < MAX_VALUES; ++i) values[i] = (byte)rng.nextInt();

    short[] primitiveCopy = new short[MAX_VALUES];
    Byte[] objCopyAuto = new Byte[MAX_VALUES];
    Byte[] objCopyNew = new Byte[MAX_VALUES];

    long primitiveElapsed = 0;
    long autoElapsed = 0;
    long newElapsed = 0;
    for (int iter = 0; iter < ITER_NUM; ++iter)
    {
      long ts1 = System.nanoTime();

      for (int i = 0; i < MAX_VALUES; ++i) primitiveCopy[i] = values[i];

      long ts2 = System.nanoTime();

      for (int i = 0; i < MAX_VALUES; ++i) objCopyAuto[i] = values[i];

      long ts3 = System.nanoTime();

      for (int i = 0; i < MAX_VALUES; ++i) objCopyNew[i] = Byte.valueOf(values[i]);

      long ts4 = System.nanoTime();

      primitiveElapsed += ts2 - ts1;
      autoElapsed += ts3 - ts2;
      newElapsed += ts4 - ts3;
    }

    printTime("byte primitive", primitiveElapsed);
    printTime("byte auto", autoElapsed);
    printTime("byte new", newElapsed);
  }

  private static void printTime(String category, long elapsed)
  {
    Formatter fmt = new Formatter();
    try
    {
      fmt.format("%20s : total %15d ns per-call %15f ns ", category, elapsed,
                 1.0 * elapsed / (MAX_VALUES * ITER_NUM));

      System.out.println(fmt.toString());
    }
    finally
    {
      fmt.close();
    }
  }

  public static void main(String[] args) throws Exception
  {
    AutoboxingPerfMain c = new AutoboxingPerfMain();
    c.testByteAutoboxing();
    c.testShortAutoboxing();
    c.testIntAutoboxing();
  }

}
