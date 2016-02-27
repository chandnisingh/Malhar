/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.splitter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.lib.io.fs.FileSplitterInput;

@StatsListener.DataQueueSize
public class SplitterThresholdRegulator implements StatsListener, Serializable
{

  private long intervalMillis = 1000L; //10 seconds

  private long acceptableBacklog = 10;

  private final Map<Integer, Long> readerBacklog = Maps.newHashMap();

  private transient long nextMillis;

  private int blocksThreshold;

  public SplitterThresholdRegulator(int blocksThreshold)
  {
    this.blocksThreshold = blocksThreshold;
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    List<Stats.OperatorStats> lastWindowedStats = stats.getLastWindowedStats();
    boolean isReader = false;

    if (lastWindowedStats != null && lastWindowedStats.size() > 0) {
      for (int i = lastWindowedStats.size() - 1; i >= 0; i--) {
        Object counters = lastWindowedStats.get(i).counters;

        if (counters != null) {
          @SuppressWarnings("unchecked")
          BasicCounters<MutableLong> lcounters = (BasicCounters<MutableLong>)counters;

          if (lcounters.getCounter(AbstractBlockReader.ReaderCounterKeys.BYTES) != null) {
            isReader = true;
            readerBacklog.put(stats.getOperatorId(), (long)lastWindowedStats.get(i).inputPorts.get(0).queueSize);
          }

          break;
        }
      }
    }

    if (System.currentTimeMillis() < nextMillis) {
      return null;
    }

    if (isReader) {
      //only need to return a response for splitter
      return null;
    }

    nextMillis = System.currentTimeMillis() + intervalMillis;
    LOG.debug("Proposed NextMillis = {}", nextMillis);

    long totalBacklog = 0;
    for (Map.Entry<Integer, Long> backlog : readerBacklog.entrySet()) {
      totalBacklog += backlog.getValue();
    }

    if (totalBacklog < acceptableBacklog) {
      //increase splitter threshold  by 1
      LOG.debug("lower backlog {} {}", totalBacklog, blocksThreshold);
      blocksThreshold++;
      Response response = new Response();
      response.operatorRequests = Lists.newArrayList(new SetThresholdRequest(blocksThreshold));
      return response;

    } else if (totalBacklog > acceptableBacklog) {
      long diffBackLog = totalBacklog - acceptableBacklog;
      int newThreshold = diffBackLog > blocksThreshold ? 0 : (int)(blocksThreshold - diffBackLog);

      if (newThreshold != blocksThreshold) {
        LOG.debug("changed backlog {} {} {}", totalBacklog, blocksThreshold, newThreshold);
        blocksThreshold = newThreshold;
        Response response = new Response();
        response.operatorRequests = Lists.newArrayList(new SetThresholdRequest(newThreshold));
        return response;
      }
    }
    return  null;
  }

  public long getIntervalMillis()
  {
    return intervalMillis;
  }

  public void setIntervalMillis(long intervalMillis)
  {
    this.intervalMillis = intervalMillis;
  }

  public long getAcceptableBacklog()
  {
    return acceptableBacklog;
  }

  public void setAcceptableBacklog(long acceptableBacklog)
  {
    this.acceptableBacklog = acceptableBacklog;
  }

  public int getBlocksThreshold()
  {
    return blocksThreshold;
  }

  public void setBlocksThreshold(int blocksThreshold)
  {
    this.blocksThreshold = blocksThreshold;
  }

  private static class SetThresholdRequest implements OperatorRequest, Serializable
  {
    private final int threshold;

    public SetThresholdRequest(int threshold)
    {
      this.threshold = threshold;
    }

    @Override
    public OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
    {
      if (operator instanceof FileSplitterInput) {
        ((FileSplitterInput) operator).setBlocksThreshold(threshold);
      }
      return null;
    }

    private static final long serialVersionUID = 201503231644L;
  }

  private static final long serialVersionUID = 201502130023L;

  private static final Logger LOG = LoggerFactory.getLogger(SplitterThresholdRegulator.class);
}
