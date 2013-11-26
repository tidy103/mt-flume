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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.Disposable;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 * DualChannel is the mixed channel of MemoryChannel and FileChannel.
 * Important: @InterfaceAudience.Private and @Disposable is from FileChannel, 
 *            espically Disposable decided DualChannel created or not when reconfiguration.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
@Disposable
public class DualChannel extends BasicChannelSemantics {
  private static Logger LOG = LoggerFactory.getLogger(DualChannel.class);

  /***
   * putToMemChannel indicate put event to memChannel or fileChannel
   * takeFromMemChannel indicate take event from memChannel or fileChannel
   * */
  private AtomicBoolean putToMemChannel = new AtomicBoolean(true);
  private AtomicBoolean takeFromMemChannel = new AtomicBoolean(true);
  private MemoryChannel memChannel = new MemoryChannel();
  private final ThreadLocal<BasicTransactionSemantics> memTransactions = new ThreadLocal<BasicTransactionSemantics>();
  private FileChannel fileChannel = new FileChannel();
  private final ThreadLocal<BasicTransactionSemantics> fileTransactions = new ThreadLocal<BasicTransactionSemantics>();
  
  private AtomicLong handleEventCount = new AtomicLong();
  private AtomicLong memHandleEventCount = new AtomicLong();
  private AtomicLong fileHandleEventCount = new AtomicLong();
  private ChannelCounter channelCounter;
  private boolean switchon = true;
  
  public DualChannel() {
	super(); 
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
	
	this.switchon = context.getBoolean("switchon", true);
	if(! switchon){
	    LOG.info("DualChannel switchon set off, will always put events to file channel");
	}
	
	memChannel.setName(getName() + "-memory");
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
	
	fileChannel.setName(getName() + "-file");
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
	super.start();
	memChannel.start();
	fileChannel.start();
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
	private final BasicTransactionSemantics memTransaction;
	private final BasicTransactionSemantics fileTransaction;
	 
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
      handleEventCount.incrementAndGet();

      if (switchon && putToMemChannel.get()) {
        memHandleEventCount.incrementAndGet();
    	memTransaction.put(event);

        /**
         * check whether memChannel queueRemaining to 30%, or fileChannel has event? 
         * if true, change to fileChannel next event.
         * */
        if ( memChannel.isFull() || fileChannel.getQueueSize() > 100) {
          putToMemChannel.set(false);
        }
      } else {
    	fileHandleEventCount.incrementAndGet();
    	fileTransaction.put(event);
      }
    }

    @Override
    protected Event doTake() throws InterruptedException {
      channelCounter.incrementEventTakeAttemptCount();

      Event event = null;
      if ( takeFromMemChannel.get() ) {
        event = memTransaction.take();
        if (event == null) {
          takeFromMemChannel.set(false);
        } 
      } else {
    	event = fileTransaction.take();
        if (event == null) {
          takeFromMemChannel.set(true);
          
          putToMemChannel.set(true);
        } 
      }
      
      return event;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      memTransaction.commit();
      fileTransaction.commit();

      //print stat information
      if (handleEventCount.get() >= 50000) {
        String msg = String.format("DualChannel-STAT name[%s] " + 
                "totalEvent[%d] memEvent[%d] fileEvent[%d] " + 
                "memQueueSize[%d] fileQueueSize[%d]", 
                getName(), handleEventCount.get(), 
                memHandleEventCount.get(), fileHandleEventCount.get(),
                memChannel.getQueueSize(), fileChannel.getQueueSize());
         LOG.info(msg); 
         handleEventCount.set(0);
         memHandleEventCount.set(0);
         fileHandleEventCount.set(0);
      }
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
