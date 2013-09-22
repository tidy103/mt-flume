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

package org.apache.flume.sink.kafka;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);

    private String zkConnect;
    private Integer zkTimeout;
    private String topic;
    private Integer batchSize;
    private Integer queueSize;
    private String serializerClass;
    private String producerType;

    private Producer<String, String> producer;

    @Override
    public void configure(Context context) {
        this.zkConnect = context.getString("zkConnect");
        Preconditions.checkNotNull(zkConnect, "zkConnect is required.");
        this.zkTimeout = context.getInteger("zkTimeout", 30000);
        this.topic = context.getString("topic");
        Preconditions.checkNotNull(topic, "topic is required.");
        this.batchSize = context.getInteger("batchSize", 600);
        this.queueSize = context.getInteger("queueSize", 100000);
        this.serializerClass = context.getString("serializerClass", "kafka.serializer.StringEncoder");
        this.producerType = context.getString("producerType", "async");
    }

    @Override
    public synchronized void start() {
        super.start();

		Properties props = new Properties();
		props.put("serializer.class", this.serializerClass);
		props.put("zk.connect", this.zkConnect);
		props.put("producer.type", this.producerType);
		props.put("batch.size", this.batchSize);
		props.put("zk.sessiontimeout.ms", this.zkTimeout);
		props.put("queue.size", this.queueSize);

		producer = new Producer<String, String>(new ProducerConfig(props));
    }

    @Override
    public synchronized void stop() {
        super.stop();
        producer.close();
    }

    @Override
    public Status process() throws EventDeliveryException {

        Status status = Status.READY;

        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        try {
            tx.begin();

            ArrayList<ProducerData<String, String>> list = new ArrayList<ProducerData<String, String>>();
            int txnEventCount = 0;
            for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
                Event event = channel.take();
                if (event == null) {
                	break;
                }         
                
                ProducerData<String, String> kafkaData = new ProducerData<String, String>(this.topic, new String(event.getBody()));
                list.add(kafkaData);             
            }
            
            producer.send(list);  

            tx.commit();
        } catch (Exception e) {
            logger.error("can't process events, drop it!", e);
            tx.rollback();
            throw new EventDeliveryException(e);
        } finally {
        	tx.close();
        }
        return status;
    }
}
