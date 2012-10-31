package com.linkedin.databus.core.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Random;

public class ByteBufferPerfMain
{
  /**
   * Number of iterations of each test
   */
  public static final int NUM_ITERS = 50;

  /**
   * Number of perf tests to be run on reads / writes
   * Currently they are writes, writes w/ autobox, writes w/o autobox, non-direct byte buffer writes, direct byte buffer write, memory mapped byte buffer write
   */
  public static final int NUM_METRICS = 6;

  /**
   * Number of samples to take during read / write
   */
  public static final int NUM_SAMPLES = 1000;


  private int[] data;
  private int[] intArray;
  private List<Integer> intList;
  private List<Integer> intList2;
  private ByteBuffer gcBuffer;
  private ByteBuffer directBuffer;
  private ByteBuffer nativeBuffer;
  private ByteBuffer mmapBuffer;

  /**
   * For metrics at higher ranges, increase MAX_ORDER.
   * To run as a standalone java exec, uncomment main code, at end of file, and remove annotation above
   */
  public void testReadWritePerf()
  {
	  // 1 KB = (2 ** 0) * (2 ** 10)
	  final int INIT_MEMORY_SIZE = 1024;

	  // 256 MB = (2 ** 18) * (2 * 10)
	  final int MAX_ORDER   = 17;

	  // Max size of test
	  final int MAX_MEMORY_SIZE = 1024 * INIT_MEMORY_SIZE ;

	  final long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
	  final long heapMemory = Runtime.getRuntime().totalMemory() / (1024 * 1024);
	  final long freeMemory = Runtime.getRuntime().freeMemory() / (1024 * 1024);

	  Formatter f = new Formatter();
	  try
	  {
    	  f.format("Memory : max / heap / free in the system is (%dMB), (%dMB), (%dMB) respectively",
    	  			maxMemory, heapMemory, freeMemory);
    	  System.out.println(f.toString());
	  }
	  finally
	  {
	    f.close();
	  }


	  data = new int[MAX_MEMORY_SIZE];
	  intArray = new int[MAX_MEMORY_SIZE];
	  intList = new ArrayList<Integer>(MAX_MEMORY_SIZE);
	  intList2 = new ArrayList<Integer>(MAX_MEMORY_SIZE);
	  gcBuffer = ByteBuffer.allocate(MAX_MEMORY_SIZE * 4);
	  directBuffer = ByteBuffer.allocateDirect(MAX_MEMORY_SIZE * 4);
	  mmapBuffer = allocateMmapBuffer(MAX_MEMORY_SIZE * 4);
      nativeBuffer = ByteBuffer.allocateDirect(MAX_MEMORY_SIZE * 4);
      nativeBuffer.order(ByteOrder.nativeOrder());

	  Random rng = new Random();
	  for (int i = 0; i < MAX_MEMORY_SIZE; ++i)
	  {
		  int num = rng.nextInt();
		  data[i] = num;
		  intArray[i] = num;
		  intList.add(num);
		  intList2.add(num);
		  gcBuffer.putInt(i * 4, num);
		  directBuffer.putInt(i * 4, num);
		  nativeBuffer.putInt(i * 4, num);
		  mmapBuffer.putInt(i * 4, num);
	  }

	  for(int order = 0; order <= MAX_ORDER; order++)
	  {
		  final int ms = INIT_MEMORY_SIZE << order;

		  List<Double> seqReadResults, randomReadResults;
		  seqReadResults      = getReadPerf(ms);
		  randomReadResults   = getrandomReadPerf(ms);

		  Formatter fmt3 = new Formatter();
		  try
		  {
		    fmt3.format("Read performance for memory buffer (2**%d) KB%n%n", order  );
		    System.out.println(fmt3.toString());
		  }
		  finally
		  {
		    fmt3.close();
		  }

		  Formatter fmt4 = new Formatter();
		  try
		  {
		    fmt4.format("int array reads              || %10.9f || %10.3f || %10.3f ||\n", seqReadResults.get(0), randomReadResults.get(0), randomReadResults.get(0)/seqReadResults.get(0));
		    fmt4.format("int list reads w/ autobox    || %10.9f || %10.3f || %10.3f ||\n", seqReadResults.get(1), randomReadResults.get(1), randomReadResults.get(1)/seqReadResults.get(1));
		    fmt4.format("int list reads w/o autobox   || %10.9f || %10.3f || %10.3f ||\n", seqReadResults.get(2), randomReadResults.get(2), randomReadResults.get(2)/seqReadResults.get(2));
		    fmt4.format("non-direct byte buffer reads || %10.9f || %10.3f || %10.3f ||\n", seqReadResults.get(3), randomReadResults.get(3), randomReadResults.get(3)/seqReadResults.get(3));
		    fmt4.format("direct byte buffer reads     || %10.9f || %10.3f || %10.3f ||\n", seqReadResults.get(4), randomReadResults.get(4), randomReadResults.get(4)/seqReadResults.get(4));
		    fmt4.format("mmapped byte buffer reads    || %10.9f || %10.3f || %10.3f ||\n", seqReadResults.get(5), randomReadResults.get(5), randomReadResults.get(5)/seqReadResults.get(5));
		    fmt4.format("native byte buffer reads    || %10.9f || %10.3f || %10.3f ||\n", seqReadResults.get(6), randomReadResults.get(6), randomReadResults.get(6)/seqReadResults.get(6));
		    System.out.println(fmt4.toString());
		  }
		  finally
		  {
		    fmt4.close();
		  }

		  /**
		   * Probably would not help, but for fun ...
		   */
		  doGarbageCollect();
	  }

          doGarbageCollect();

	  for(int order = 0; order <= MAX_ORDER; order++)
	  {
		  final int ms = INIT_MEMORY_SIZE << order;

		  List<Double> seqWriteResults, randomWriteResults;
		  seqWriteResults     = getWritePerf(ms);
		  randomWriteResults  = getRandomWritePerf(ms);

		  Formatter fmt1 = new Formatter();
		  try
		  {
		    fmt1.format("Write performance for memory buffer (2**%d) KB%n%n", order  );
		    System.out.println(fmt1.toString());
		  }
		  finally
		  {
		    fmt1.close();
		  }

		  Formatter fmt2 = new Formatter();
		  try
		  {
		    fmt2.format("int array writes              || %10.3f || %10.3f || %10.3f  ||%n", seqWriteResults.get(0), randomWriteResults.get(0), randomWriteResults.get(0)/seqWriteResults.get(0));
		    fmt2.format("int list writes w/ autobox    || %10.3f || %10.3f || %10.3f  ||%n", seqWriteResults.get(1), randomWriteResults.get(1), randomWriteResults.get(1)/seqWriteResults.get(1));
		    fmt2.format("int list writes w/o autobox   || %10.3f || %10.3f || %10.3f  ||%n", seqWriteResults.get(2), randomWriteResults.get(2), randomWriteResults.get(2)/seqWriteResults.get(2));
		    fmt2.format("non-direct byte buffer writes || %10.3f || %10.3f || %10.3f  ||%n", seqWriteResults.get(3), randomWriteResults.get(3), randomWriteResults.get(3)/seqWriteResults.get(3));
		    fmt2.format("direct byte buffer writes     || %10.3f || %10.3f || %10.3f  ||%n", seqWriteResults.get(4), randomWriteResults.get(4), randomWriteResults.get(4)/seqWriteResults.get(4));
		    fmt2.format("mmapped byte buffer writes    || %10.3f || %10.3f || %10.3f  ||%n", seqWriteResults.get(5), randomWriteResults.get(5), randomWriteResults.get(5)/seqWriteResults.get(5));
		    fmt2.format("native byte buffer writes    || %10.3f || %10.3f || %10.3f  ||%n", seqWriteResults.get(6), randomWriteResults.get(6), randomWriteResults.get(5)/seqWriteResults.get(6));
		    System.out.println(fmt2.toString());
		  }
		  finally
		  {
		    fmt2.close();
		  }

		  doGarbageCollect();
	  }


  }

  /**
   * This test measures the sequential write performance, for a given sized memory buffer
   */
  private void doGarbageCollect()
  {
	  // Invoke garbage collector to free out the allocated buffers,
	  // as memory allocation seems to fail at higher sizes
	  System.runFinalization();
  }

  /**
   * This test measures the sequential write performance, for a given sized memory buffer
   */
  private List<Double> getWritePerf(final int MEMORY_SIZE)
  {
    /**
     * Store and return results in an array for processed output
     */
    List<Double> results = new ArrayList<Double>(NUM_METRICS);

    long ts1s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) intArray[i] = data[i];
    }
    long ts1f = System.nanoTime();

    long ts2s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) intList.set(i, data[i]);
    }
    long ts2f = System.nanoTime();

    long ts3s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) gcBuffer.putInt(i * 4, data[i]);
    }
    long ts3f = System.nanoTime();

    long ts4s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) directBuffer.putInt(i * 4, data[i]);
    }
    long ts4f = System.nanoTime();

    long ts5s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) intList.set(i, intList2.get(i));
    }
    long ts5f = System.nanoTime();

    long ts6s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) mmapBuffer.putInt(i * 4, data[i]);
    }
    long ts6f = System.nanoTime();

    long ts7s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) nativeBuffer.putInt(i * 4, data[i]);
    }
    long ts7f = System.nanoTime();

    final int DEN = NUM_SAMPLES * NUM_ITERS;
    results.add(0, (1.0 * (ts1f - ts1s)) / DEN);
    results.add(1, (1.0 * (ts2f - ts2s)) / DEN);
    results.add(2, (1.0 * (ts5f - ts5s)) / DEN);
    results.add(3, (1.0 * (ts3f - ts3s)) / DEN);
    results.add(4, (1.0 * (ts4f - ts4s)) / DEN);
    results.add(5, (1.0 * (ts6f - ts6s)) / DEN);
    results.add(6, (1.0 * (ts7f - ts7s)) / DEN);

    return results;
  }

  /**
   * This test measures the random write performance, for a given sized memory buffer
   */
  private  List<Double> getRandomWritePerf(final int MEMORY_SIZE)
  {

    /**
     * Store and return results in an array for processed output
     */
    List<Double> results = new ArrayList<Double>(NUM_METRICS);

    Random rng = new Random();
    long delta = 0;

    long ts1s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	long s = System.nanoTime();
        int index = rng.nextInt(MEMORY_SIZE);
        long e = System.nanoTime();
        delta += e -s;
        intArray[index] = data[index];
      }
    }
    long ts1f = System.nanoTime() - delta;

    delta = 0;
    long ts2s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	long s = System.nanoTime();
        int index = rng.nextInt(MEMORY_SIZE);
        long e = System.nanoTime();
        delta += e - s;
        intList.set(index, data[i]);
      }
    }
    long ts2f = System.nanoTime() - delta;

    delta = 0;
    long ts3s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	long s = System.nanoTime();
        int index = rng.nextInt(MEMORY_SIZE);
        long e = System.nanoTime();
        delta += e - s;
        gcBuffer.putInt(index * 4, data[index]);
      }
    }
    long ts3f = System.nanoTime() - delta;

    delta = 0;
    long ts4s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	long s = System.nanoTime();
        int index = rng.nextInt(MEMORY_SIZE);
        long e = System.nanoTime();
        delta += e - s;
        directBuffer.putInt(index * 4, data[index]);
      }
    }
    long ts4f = System.nanoTime() - delta;

    delta = 0;
    long ts5s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	long s = System.nanoTime();
        int index = rng.nextInt(MEMORY_SIZE);
        long e = System.nanoTime();
        delta += e - s;
        intList.set(index, intList2.get(index));
      }
    }
    long ts5f = System.nanoTime() - delta;
    delta = 0;

    long ts6s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	long s = System.nanoTime();
        int index = rng.nextInt(MEMORY_SIZE);
        long e = System.nanoTime();
        delta += e - s;
        mmapBuffer.putInt(index * 4, data[index]);
      }
    }
    long ts6f = System.nanoTime() - delta;
    delta = 0;

    long ts7s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
        long s = System.nanoTime();
        int index = rng.nextInt(MEMORY_SIZE);
        long e = System.nanoTime();
        delta += e - s;
        nativeBuffer.putInt(index * 4, data[index]);
      }
    }
    long ts7f = System.nanoTime() - delta;
    delta = 0;

    final int DEN = NUM_SAMPLES * NUM_ITERS;
    results.add(0, (1.0 * (ts1f - ts1s)) / DEN);
    results.add(1, (1.0 * (ts2f - ts2s)) / DEN);
    results.add(2, (1.0 * (ts5f - ts5s)) / DEN);
    results.add(3, (1.0 * (ts3f - ts3s)) / DEN);
    results.add(4, (1.0 * (ts4f - ts4s)) / DEN);
    results.add(5, (1.0 * (ts6f - ts6s)) / DEN);
    results.add(6, (1.0 * (ts7f - ts7s)) / DEN);

    return results;
  }

  /**
   * This test measures the sequential write performance, for a given sized memory buffer
   */
  @SuppressWarnings("unused")
  private  List<Double> getReadPerf(final int MEMORY_SIZE)
  {
    /**
     * Store and return results in an array for processed output
     */
    List<Double> results = new ArrayList<Double>(NUM_METRICS);

    int n;
    Integer N;

    long ts1s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) n = intArray[i];
    }
    long ts1f = System.nanoTime();

    long ts2s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) n = intList.get(i);
    }
    long ts2f = System.nanoTime();

    long ts3s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) n = gcBuffer.getInt(i * 4);
    }
    long ts3f = System.nanoTime();

    long ts4s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) n = directBuffer.getInt(i * 4);
    }
    long ts4f = System.nanoTime();

    long ts5s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) N = intList2.get(i);
    }
    long ts5f = System.nanoTime();

    long ts6s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) n = mmapBuffer.getInt(i * 4);
    }
    long ts6f = System.nanoTime();

    long ts7s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i) n = nativeBuffer.getInt(i * 4);
    }
    long ts7f = System.nanoTime();


    // Reads take too little time, compute time taken for NUM_SAMPLES reads
    //final int DEN = NUM_ITERS * NUM_SAMPLES;
    final int DEN = NUM_ITERS;
    results.add(0, (1.0 * (ts1f - ts1s)) / DEN);
    results.add(1, (1.0 * (ts2f - ts2s)) / DEN);
    results.add(2, (1.0 * (ts5f - ts5s)) / DEN);
    results.add(3, (1.0 * (ts3f - ts3s)) / DEN);
    results.add(4, (1.0 * (ts4f - ts4s)) / DEN);
    results.add(5, (1.0 * (ts6f - ts6s)) / DEN);
    results.add(6, (1.0 * (ts7f - ts7s)) / DEN);

    return results;
  }

  /**
   * Measures random read performance in a given memory sized buffer
   */
  private  List<Double> getrandomReadPerf(final int MEMORY_SIZE)
  {
    /**
     * Store and return results in an array for processed output
     */
    List<Double> results = new ArrayList<Double>(NUM_METRICS);

    Random rng = new Random();

    int n = 0;
    Integer N = 0;

    /**
     * Account for time taken for random number generations
     */
    long delta;

    delta = 0;
    long ts1s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	  long s = System.nanoTime();
    	  int index = rng.nextInt(MEMORY_SIZE);
    	  long e = System.nanoTime();
    	  delta += e - s;
    	  n = intArray[index];
      }
    }
    long ts1f = System.nanoTime() - delta;

    delta = 0;
    long ts2s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	  long s = System.nanoTime();
          int index = rng.nextInt(MEMORY_SIZE);
          long e = System.nanoTime();
          delta += e - s;
    	  n = intList.get(index);
      }
    }
    long ts2f = System.nanoTime() - delta;

    delta = 0;
    long ts3s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	  long s = System.nanoTime();
          int index = rng.nextInt(MEMORY_SIZE);
          long e = System.nanoTime();
          delta += e - s;
    	  n = gcBuffer.getInt(index * 4);
      }
    }
    long ts3f = System.nanoTime() - delta;

    delta = 0;
    long ts4s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	  long s = System.nanoTime();
          int index = rng.nextInt(MEMORY_SIZE);
          long e = System.nanoTime();
          delta += e - s;
    	  n = directBuffer.getInt(index * 4);
      }
    }
    long ts4f = System.nanoTime() - delta;

    delta = 0;
    long ts5s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	  long s = System.nanoTime();
          int index = rng.nextInt(MEMORY_SIZE);
          long e = System.nanoTime();
          delta += e - s;
    	  N = intList2.get(index);
      }
    }
    long ts5f = System.nanoTime() - delta;

    delta = 0;
    long ts6s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
    	  long s = System.nanoTime();
          int index = rng.nextInt(MEMORY_SIZE);
          long e = System.nanoTime();
          delta += e - s;
    	  n = mmapBuffer.getInt(index * 4);
      }
    }
    long ts6f = System.nanoTime() - delta;

    delta = 0;
    long ts7s = System.nanoTime();
    for(int iter = 0; iter < NUM_ITERS; ++iter)
    {
      for (int i = 0; i < NUM_SAMPLES; ++i)
      {
          long s = System.nanoTime();
          int index = rng.nextInt(MEMORY_SIZE);
          long e = System.nanoTime();
          delta += e - s;
          n = nativeBuffer.getInt(index * 4);
      }
    }
    long ts7f = System.nanoTime() - delta;

    //final int DEN = NUM_SAMPLES * NUM_ITERS;
    final int DEN = NUM_ITERS;
    results.add(0, (1.0 * (ts1f - ts1s)) / DEN);
    results.add(1, (1.0 * (ts2f - ts2s)) / DEN);
    results.add(2, (1.0 * (ts5f - ts5s)) / DEN);
    results.add(3, (1.0 * (ts3f - ts3s)) / DEN);
    results.add(4, (1.0 * (ts4f - ts4s)) / DEN);
    results.add(5, (1.0 * (ts6f - ts6s)) / DEN);
    results.add(6, (1.0 * (ts7f - ts7s)) / DEN);

    System.out.println("ignore: " + n + "," + N);

    return results;
  }

  private ByteBuffer allocateMmapBuffer(final int maxBufferSize)
  {
	  try
	  {
		  final File readFile = File.createTempFile("temp", Long.toString(System.nanoTime()));
		  ByteBuffer buf;
		  RandomAccessFile raf = null;
		  FileChannel rwChannel= null;
		  try
		  {
		    raf = new RandomAccessFile(readFile, "rw");
		    rwChannel = raf.getChannel();
		    buf = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxBufferSize);
		    readFile.deleteOnExit();
		    return buf;
		  }
		  finally
		  {
		    if (null != raf) raf.close();
		    if (null != rwChannel) rwChannel.close();
		  }
	  }
	  catch (FileNotFoundException e)
	  {
		  throw new RuntimeException(e);
	  }
	  catch (IOException e)
	  {
		  throw new RuntimeException(e);
	  }
  }

  public static void main(String [] args)
  {
  	ByteBufferPerfMain tb = new ByteBufferPerfMain();
    tb.testReadWritePerf();
  }

}
