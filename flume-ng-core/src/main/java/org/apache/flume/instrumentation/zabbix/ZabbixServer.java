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
package org.apache.flume.instrumentation.zabbix;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZabbixServer implements MonitorService {
    private static final Logger logger = LoggerFactory
            .getLogger(ZabbixServer.class);

    private String hostname;
    private ScheduledExecutorService service =
            Executors.newSingleThreadScheduledExecutor();
    protected final ZabbixCollector collectorRunnable;
    private Map<String, Integer> zabbixServers = new HashMap<String, Integer>();
    private int pollFrequency = 60;
    public final String ZABBIX_CONTEXT = "flume.";
    public final String CONF_POLL_FREQUENCY = "pollFrequency";
    public final int DEFAULT_POLL_FREQUENCY = 60;
    public final String CONF_HOSTS = "hosts";
    
    
    public ZabbixServer(){
        collectorRunnable = new ZabbixCollector();
    }

    @Override
    public void configure(Context context) {
        // TODO Auto-generated method stub
        this.pollFrequency = context.getInteger(this.CONF_POLL_FREQUENCY, DEFAULT_POLL_FREQUENCY);
        //zabbix hosts
        String hosts = context.getString(this.CONF_HOSTS);
        if (hosts == null || hosts.isEmpty()) {
            throw new ConfigurationException("Hosts list cannot be empty.");
        }
        parseHostsFromString(hosts);

    }

    private void parseHostsFromString(String hosts)
            throws FlumeException {
        
        String[] hostsAndPorts = hosts.split(",");
        for (String host : hostsAndPorts) {
            String[] hostAndPort = host.split(":");
            if (hostAndPort.length < 2) {
                logger.warn("Invalid zabbix host: ", host);
                continue;
            }
            try {               
                zabbixServers.put(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
            } catch (Exception e) {
                logger.warn("Invalid zabbix host: " + host, e);
                continue;
            }
        }
        if (zabbixServers.isEmpty()) {
            throw new FlumeException("No valid zabbix hosts defined!");
        }
    }

    @Override
    public void start() {
        // TODO Auto-generated method stub
        try {
            hostname = InetAddress.getLocalHost().getHostName().split("\\.")[0];
            }catch (Exception ex2) {
            logger.warn("Unknown error occured", ex2);
          }
        
        collectorRunnable.server = this;
        if (service.isShutdown() || service.isTerminated()) {
          service = Executors.newSingleThreadScheduledExecutor();
        }
        service.scheduleWithFixedDelay(collectorRunnable, 0,
                pollFrequency, TimeUnit.SECONDS);

    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub

        service.shutdown();

        while (!service.isTerminated()) {
          try {
            logger.warn("Waiting for zabbix service to stop");
            service.awaitTermination(500, TimeUnit.MILLISECONDS);
          } catch (InterruptedException ex) {
            logger.warn("Interrupted while waiting"
                    + " for zabbix monitor to shutdown", ex);
            service.shutdownNow();
          }
        }
        
        zabbixServers.clear();
        
    }
    
    
    public void send(List<ZabbixItem> itemList) throws Exception{
        
        ZabbixSender sender = new ZabbixSender(itemList, zabbixServers, hostname);
        sender.send();
        
    }

    /**
     * Worker which polls JMX for all mbeans with
     * {@link javax.management.ObjectName} within the flume namespace:
     * org.apache.flume. All attributes of such beans are sent to the all hosts
     * specified by the server that owns it's instance.
     * 
     */
    protected class ZabbixCollector implements Runnable {

        private ZabbixServer server;
        private final MBeanServer mbeanServer = ManagementFactory
                .getPlatformMBeanServer();

        @Override
        public void run() {
            try {
                
                Map<String, Map<String, String>> metricsMap = JMXPollUtil
                        .getAllMBeans();
                List<ZabbixItem> itemList = new ArrayList<ZabbixItem>();
                for (String component : metricsMap.keySet()) {
                    Map<String, String> attributeMap = metricsMap
                            .get(component);
                    for (String attribute : attributeMap.keySet()) {
                        if(MetricFilter.accept(component, attribute)){
                            String value = attributeMap.get(attribute);
                            attribute = ZABBIX_CONTEXT + component + "." + attribute;
                            ZabbixItem item = new ZabbixItem(attribute, value, hostname);
                            itemList.add(item);
                            logger.debug(item.toString());
                        }
                    }
                }
                server.send(itemList);
            } catch (Throwable t) {
                logger.error("Unexpected error", t);
            }
        }
    }

}
