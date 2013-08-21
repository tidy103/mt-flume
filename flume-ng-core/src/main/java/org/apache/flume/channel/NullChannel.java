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

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.annotations.Recyclable;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * NullChannel is the recommended channel to use when speeds which
 * writing to disk is impractical is required or durability of data is not
 * required.
 * </p>
 * <p>
 * Additionally, NullChannel should be used when a channel is required for
 * unit testing purposes.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Recyclable
public class NullChannel extends BasicChannelSemantics {
  private static Logger LOGGER = LoggerFactory.getLogger(NullChannel.class);
  
  private static final Integer defaultKeepAlive = 3;

  private class NullTransaction extends BasicTransactionSemantics {
    private final ChannelCounter channelCounter;

    public NullTransaction(ChannelCounter counter) {
      channelCounter = counter;
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
      channelCounter.incrementEventPutAttemptCount();
    }

    @Override
    protected Event doTake() throws InterruptedException {
      channelCounter.incrementEventTakeAttemptCount();
      return null;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      //do nothing
    }

    @Override
    protected void doRollback() {
      //do nothing
    }

  }
  
  private ChannelCounter channelCounter;

  public NullChannel() {
    super();
  }

  @Override
  public void configure(Context context) {
    if (channelCounter == null) {
      channelCounter = new ChannelCounter(getName());
    }
  }

  @Override
  public synchronized void start() {
    channelCounter.start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    channelCounter.stop();
    super.stop();
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new NullTransaction(channelCounter);
  }
}
