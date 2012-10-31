package com.linkedin.databus2.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public class InflateMain
{
  public static final int BUF_SIZE = 32 * 1024 * 1024;
  final BlockingQueue<byte[]> _bufs;
  final InputStream _is;
  final OutputStream _os;
  volatile boolean done = false;

  public InflateMain(InputStream is, OutputStream os)
  {
    long memSize = Runtime.getRuntime().freeMemory();
    long bufMem = (long)(0.70 * memSize);
    int bufNum = (int)(bufMem / BUF_SIZE);

    _bufs = new ArrayBlockingQueue<byte[]>(bufNum);
    _is = is;
    _os = os;
  }

  public void inflate()
  {
    Thread inflateThread = new Thread(new InflatorRunnable(), "inflator");
    Thread writeThread = new Thread(new WriterRunnable(), "writer");

    writeThread.start();
    inflateThread.start();

    try
    {
      inflateThread.join();
    }
    catch (InterruptedException e) { done = true;}

    try {writeThread.join();} catch (InterruptedException e) {}
    System.err.println(" Done. ");
  }

  public class InflatorRunnable implements Runnable
  {

    @Override
    public void run()
    {
      //System.err.print (" Inflating ");
      int res = 0;
      do
      {
        byte[] b1 = new byte[BUF_SIZE];
        byte[] b = null;
        try
        {
          res = _is.read(b1);
          if (res >0)
          {
            System.err.print("<");
            if (res < BUF_SIZE)
            {
              b = new byte[res];
              System.arraycopy(b1, 0, b, 0, res);
            }
            else b = b1;
            try{_bufs.put(b);} catch (InterruptedException e ) {done = true;}
          }
        }
        catch (IOException e)
        {
          System.err.println();
          System.err.println("Inflate error: " + e);
          done = true;
        }
      }
      while (!done && res > 0);

      done = true;
      //System.err.print (" Inflating done ");
    }

  }

  public class WriterRunnable implements Runnable
  {

    @Override
    public void run()
    {
      //System.err.print (" Writing ");
      while (!done || _bufs.size() > 0)
      {
        byte[] b = null;
        try {b = _bufs.poll(100, TimeUnit.MILLISECONDS);} catch (InterruptedException e) {done = true;}
        try
        {
          if (null != b)
          {
            _os.write(b, 0, b.length);
            System.err.print(">");
          }
        }
        catch (IOException e)
        {
          System.err.println();
          System.err.println("Write error: " + e);
          done = true;
        }
      }
      //System.err.print (" Writing done ");
    }

  }

  /**
   * @param args
   */
  public static void main(String[] args) throws IOException
  {
    if (args.length == 0) throw new RuntimeException("usage: java " + InflateMain.class.getSimpleName()
                                                     + " file ...");
    for (String fname: args)
    {
      FileInputStream fis = null;
      InflaterInputStream iis = null;
      FileOutputStream fos = null;
      try
      {
        File f = new File(fname);
        String fn = f.getName();
        File outf = fn.endsWith(".deflate") ?
            new File(f.getParent(), fn.substring(0, fn.length() - ".deflate".length())) :
            new File(f.getParent(), fn + ".inflated");

        fis = new FileInputStream(f);
        iis =  new InflaterInputStream(fis, new Inflater(), BUF_SIZE);

        fos = new FileOutputStream(outf);
        System.err.print(f.getAbsolutePath() + " -> " + outf.getAbsolutePath() + " ");

        InflateMain runner = new InflateMain(iis, fos);
        runner.inflate();
      }
      finally
      {
        if (null != fos) fos.close();
        if (null != iis) iis.close();
        if (null != fis) fis.close();
      }
    }
  }

}
