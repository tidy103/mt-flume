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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
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

public class KafkaSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);

    private String brokerList;
    private Integer requestRequiredAcks;
    private Long requestTimeoutms;
    private String serializerClass;
    private String partitionerClass;
    private String producerType;
    private Integer batchNumMessages;
    private Integer queueBufferingMaxMessages;
    
    private String topicPrefix;

    private Producer<String, String> producer;

    @Override
    public void configure(Context context) {
        this.brokerList = context.getString("brokerList");
        Preconditions.checkNotNull(brokerList, "brokerList is required.");
        this.requestRequiredAcks = context.getInteger("requestRequiredAcks", 0);
        this.requestTimeoutms = context.getLong("requestTimeoutms", Long.valueOf(10000));
        this.serializerClass = context.getString("serializerClass", "kafka.serializer.StringEncoder");
        this.partitionerClass = context.getString("partitionerClass", "kafka.producer.DefaultPartitioner");
        this.producerType = context.getString("producerType", "async");
        this.batchNumMessages = context.getInteger("batchNumMessages", 200);
        this.queueBufferingMaxMessages = context.getInteger("queueBufferingMaxMessages", 1000);
        
        this.topicPrefix = context.getString("topicPrefix");
        Preconditions.checkNotNull(topicPrefix, "topicPrefix is required.");
    }

    @Override
    public synchronized void start() {
        super.start();

		Properties props = new Properties();
    	props.put("metadata.broker.list", brokerList);
    	props.put("request.required.acks", String.valueOf(requestRequiredAcks));
    	props.put("request.timeout.ms", String.valueOf(requestTimeoutms));
		props.put("serializer.class", serializerClass);
		props.put("partitioner.class", partitionerClass);
		props.put("producer.type", producerType);
		props.put("batch.num.messages", String.valueOf(batchNumMessages));
		props.put("queue.buffering.max.messages", String.valueOf(queueBufferingMaxMessages));
		props.put("topic.metadata.refresh.interval.ms", "30000");

		producer = new Producer<String, String>(new ProducerConfig(props));
    }

    @Override
    public synchronized void stop() {
        super.stop();
        if (producer != null) {
        	producer.close();
        }
    }

    @Override
    public Status process() throws EventDeliveryException {

        Status status = Status.READY;

        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        try {
            tx.begin();
            
            List<KeyedMessage<String, String>> datas = new ArrayList<KeyedMessage<String, String>>();

            int txnEventCount = 0;
            for (txnEventCount = 0; txnEventCount < batchNumMessages; txnEventCount++) {
                Event event = channel.take();
                if (event == null) {
                	break;
                }         
                Map<String, String> headers = event.getHeaders();
                if(headers == null){
                  logger.warn("headers are Null");
                  continue;
                }
                
                String topic = headers.get("category");
                if(topic == null){
                  logger.warn("headers do not contain entry of category");
                  continue;
                }
                topic = topicPrefix + "." + topic;
                
                KeyedMessage<String, String> m = new KeyedMessage<String, String>(topic, new String(event.getBody()));
                datas.add(m);
            }

            producer.send(datas);  

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
