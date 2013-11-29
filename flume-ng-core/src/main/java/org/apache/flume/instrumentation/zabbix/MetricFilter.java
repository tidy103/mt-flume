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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MetricFilter {
    
    private static Map<String, Set<String>> acceptComponent2MetricSet = new HashMap<String, Set<String>>();   
    
    static{
        Set<String> sourceMetrics = new HashSet<String>();
        sourceMetrics.add("EventReceivedCount");
        sourceMetrics.add("EventAcceptedCount");    // processed count
        acceptComponent2MetricSet.put("SOURCE", sourceMetrics);
        
        Set<String> channelMetrics = new HashSet<String>();
        channelMetrics.add("ChannelSize");
        acceptComponent2MetricSet.put("CHANNEL", channelMetrics);
        
        Set<String> sinkMetrics = new HashSet<String>();
        sinkMetrics.add("EventDrainSuccessCount");
        acceptComponent2MetricSet.put("SINK", sinkMetrics);
        
    }
    
    private static String getRealComponent(String component){
        //CHANNEL.ch_dual_0-file
        String tokens[] = component.split("\\.");
        return tokens[0];
    }
    
    public static boolean accept(String component, String metricName){
        if(component == null || component.equals("")){
            return false;
        }
        component = getRealComponent(component);
        Set<String> acceptMetricSet = acceptComponent2MetricSet.get(component);
        if(acceptMetricSet != null && acceptMetricSet.contains(metricName)){
            return true;            
        }
        
        return false;
    }

}
