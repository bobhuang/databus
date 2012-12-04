package com.linkedin.databus.tests.inprocess;

import java.io.File;
import java.util.HashSet;
import java.util.List;

public interface DbusInprocessConsumer
{

  /*
   * Returns List of error occurence in terms of elapsed Time/events seen
   */
  public abstract List<Long> getErrorOccurences();

  /*
   * Get List of SourceIds that this Consumer is interested int
   */
  public abstract HashSet<Integer> getSourceIds();

  /*
   * Returns the maxScn seen by this consumer
   */
  public abstract long maxScnConsumed();

  /*
   * Gives the Stop signal to consumer
   */
  public abstract void stopConsumption();

  /*
   * Return rate of consumption
   */
  public abstract double getRate();

  /*
   * Gives the stop signal and waits for the consumer to really stop
   */
  public abstract void stopConsumptionAndWait();

  /*
   * Used for Output comparison Mode. Returns the dump file for validation
   */
  public abstract File getDumpFile();

  /*
   * Returns the number of SCNNotFoundErrors seen
   */
  public abstract int getNumScnNotFoundErrorsSeen();

  /*
   * Has the consumer encountered any Runtime Error which caused it to stop
   */
  public abstract boolean isRuntimeError();

}
