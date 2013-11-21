package org.apache.flume.sink.hdfs;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.formatter.output.BucketPath;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class TestFormatSpeed {
    
    
    public static void regFormat(String path, String fileName, List<Event> events){
        
        long t1 = System.currentTimeMillis();
        boolean flag = true;
        for(Event event : events){
            String realPath = BucketPath.escapeString(path, event.getHeaders(),
                null, false, Calendar.SECOND, 1, true);
            String realName = BucketPath.escapeString(fileName, event.getHeaders(),
                    null, false, Calendar.SECOND, 1, true);
            
            if(flag){
                System.out.println(realPath + "/" + realName);
                flag = false;
            }
        }
        System.out.println("regFormat last : " + (System.currentTimeMillis() - t1));
    }
    
    
    public static void strFormat(String path, String fileName, List<Event> events){
        
        
        long t1 = System.currentTimeMillis();
        boolean flag = true;
        for(Event event : events){
            Calendar calendar = null;
    
            calendar = Calendar.getInstance();
    
//            String dt = String.format("%s%s%s", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1,
//                    calendar.get(Calendar.DAY_OF_MONTH));
//            String dt = String.valueOf(calendar.get(Calendar.YEAR)) + String.valueOf(calendar.get(Calendar.MONTH) + 1)
//                    + String.valueOf(calendar.get(Calendar.DAY_OF_MONTH));
            
            StringBuilder sb = new StringBuilder();
            sb.append(calendar.get(Calendar.YEAR)).append(zeroFill(calendar.get(Calendar.MONTH) + 1)).append(zeroFill(calendar.get(Calendar.DAY_OF_MONTH)));
            String dt = sb.toString();
            
            String hour = zeroFill(calendar.get(Calendar.HOUR_OF_DAY));        
//            String realPath = "/user/hive/warehouse/originallog.db/" + String.format("%sorg/dt=%s/hour=%s", event.getHeaders().get("category"),
//                    dt, hour);
            
            String realPath = "/user/hive/warehouse/originallog.db/" + event.getHeaders().get("category") + "org/dt=" + dt + "/hour=" + hour; 
            // filePrefix if fixed,  just use it
            String realName = fileName;
            
            if(flag){
                System.out.println(realPath + "/" + realName);
                flag = false;
            }
        }
        System.out.println("strFormat last : " + (System.currentTimeMillis() - t1));
    }
    
    public static String zeroFill(int num){
        if(num < 10){
            return "0" + num;
        }
        return String.valueOf(num);
      }
    
    
    public static void main(String[] args){
        
        List<Event> events = new ArrayList<Event>();
        Event event = new SimpleEvent();
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("category", "test");
        event.setHeaders(headers);
        event.setBody("".getBytes());
        for(int i = 0; i < 200000; i++){
            events.add(event);
        }
        
        String path = "/user/hive/warehouse/originallog.db/%{category}org/dt=%Y%m%d/hour=%H";
        String fileName = "lc_srv02";
        
        regFormat(path, fileName, events);       
        strFormat(path, fileName, events);
        
        
        
    }

}
