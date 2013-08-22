/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel;

import java.util.Map;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.annotations.Recyclable;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * DualChannel is the mixed channel of MemoryChannel and FileChannel.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Recyclable
public class DualChannel extends BasicChannelSemantics {
  private static Logger LOG = LoggerFactory.getLogger(DualChannel.class);
  
  private MemoryChannel memChannel = new MemoryChannel();
  private final ThreadLocal<BasicTransactionSemantics> memTransactions = new ThreadLocal<BasicTransactionSemantics>();
  private FileChannel fileChannel = new FileChannel();
  private final ThreadLocal<BasicTransactionSemantics> fileTransactions = new ThreadLocal<BasicTransactionSemantics>();
  
  private ChannelCounter channelCounter;
  
  public DualChannel() {
	super(); 
	memChannel.setName(getName() + "-memory");
	fileChannel.setName(getName() + "-file");
  }

  /**
   * Read parameters from context
   * <li>capacity = type long that defines the total number of events allowed at one time in the queue.
   * <li>transactionCapacity = type long that defines the total number of events allowed in one transaction.
   * <li>byteCapacity = type long that defines the max number of bytes used for events in the queue.
   * <li>byteCapacityBufferPercentage = type int that defines the percent of buffer between byteCapacity and the estimated event size.
   * <li>keep-alive = type int that defines the number of second to wait for a queue permit
   */
  @Override
  public void configure(Context context) {
	if (channelCounter == null) {
	  channelCounter = new ChannelCounter(getName());
	}
	
	//configure mc
	try {
	  Map<String, String> memoryParams = context.getSubProperties("memory.");
	  Context memContext = new Context();
	  memContext.putAll(memoryParams);
	  Configurables.configure(memChannel, memContext);
    } catch (Exception e) {
        String msg = String.format("DualChannel %s configure memChannel, an " +
              "error during configuration", getName());
        LOG.error(msg, e);
    }
	
	//configure fc
	try {
	  Map<String, String> fileParams = context.getSubProperties("file.");
	  Context fileContext = new Context();
	  fileContext.putAll(fileParams);
	  Configurables.configure(fileChannel, fileContext);
	} catch (Exception e) {
	  String msg = String.format("DualChannel %s configure fileChannel, an " +
	              "error during configuration", getName());
	  LOG.error(msg, e);
	}
  }

  @Override
  public synchronized void start() {
	channelCounter.start();
	//channelCounter.setChannelSize(queue.size());
	//channelCounter.setChannelCapacity(Long.valueOf(
	//            queue.size() + queue.remainingCapacity()));  
	memChannel.start();
	fileChannel.start();
	super.start();
  }

  @Override
  public synchronized void stop() {
	//channelCounter.setChannelSize(queue.size());
	//channelCounter.stop();  
	memChannel.stop();
	fileChannel.stop();
    super.stop();
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
	//create MemoryTransaction  
    BasicTransactionSemantics curMemTransaction = memTransactions.get();
    if (curMemTransaction == null || curMemTransaction.getState().equals(
            BasicTransactionSemantics.State.CLOSED)) {
      curMemTransaction = memChannel.createTransaction();
      memTransactions.set(curMemTransaction);
    }
    //create FileTransaction
    BasicTransactionSemantics curfileTransaction = fileTransactions.get();
    if (curfileTransaction == null || curfileTransaction.getState().equals(
            BasicTransactionSemantics.State.CLOSED)) {
      curfileTransaction = fileChannel.createTransaction();
      fileTransactions.set(curfileTransaction);
    }
	
    return new DualTransaction(curMemTransaction, curfileTransaction, channelCounter);
  }
  
  private class DualTransaction extends BasicTransactionSemantics {
	BasicTransactionSemantics memTransaction;
	BasicTransactionSemantics fileTransaction;
	  
    private final ChannelCounter channelCounter;

    public DualTransaction(BasicTransactionSemantics mt, BasicTransactionSemantics ft, 
    						ChannelCounter counter) {
      memTransaction = mt;
      fileTransaction = ft;

      channelCounter = counter;
    }
    
    @Override
    protected void doBegin() throws InterruptedException {
      memTransaction.begin();
      fileTransaction.begin();
      super.doBegin();
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
      channelCounter.incrementEventPutAttemptCount();

      try {
    	  memTransaction.put(event);
      } catch (ChannelException ce) {
    	  LOG.warn(ce.getMessage());
    	  fileTransaction.put(event);
      }	  
    }

    @Override
    protected Event doTake() throws InterruptedException {
      channelCounter.incrementEventTakeAttemptCount();

      Event event = null;
      event = memTransaction.take();
      if (event == null) {
    	  event = fileTransaction.take();
      }
      
      return event;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      memTransaction.commit();
      fileTransaction.commit();
    }

    @Override
    protected void doRollback() throws InterruptedException {
      memTransaction.rollback();
      fileTransaction.rollback();
    }

    @Override
    protected void doClose() {
      memTransaction.close();
      fileTransaction.close();
      super.doClose();
    }
  }


}
