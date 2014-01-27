package org.apache.flume.sink.hdfs;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.formatter.output.BucketPath;
import org.junit.Before;
import org.junit.Test;

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
    private List<Event> events;
    
    @Before
    public void setUp(){
        events = new ArrayList<Event>();
        Event event = new SimpleEvent();
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("category", "test");
        event.setHeaders(headers);
        event.setBody("".getBytes());
        for(int i = 0; i < 200000; i++){
            events.add(event);
        }
        
    }
    
    @Test
    public void testRegFormat(){
        String path = "/user/hive/warehouse/originallog.db/%{category}org/dt=%Y%m%d/hour=%H";
        String fileName = "lc_srv02";
        regFormat(path, fileName);       
    }
    
    public void regFormat(String path, String fileName){
        
        long t1 = System.currentTimeMillis();
        for(Event event : events){
            String realPath = BucketPath.escapeString(path, event.getHeaders(),
                null, false, Calendar.SECOND, 1, true);
            String realName = BucketPath.escapeString(fileName, event.getHeaders(),
                    null, false, Calendar.SECOND, 1, true);
        }
        System.out.println("regFormat last : " + (System.currentTimeMillis() - t1));
    }
    
    @Test
    public void testStrFormat(){
        String path = "/user/hive/warehouse/originallog.db/";
        String fileName = "lc_srv02";
        strFormat(path, fileName);
    }
    
    public void strFormat(String path, String fileName){      
        
        long t1 = System.currentTimeMillis();
        for(Event event : events){       
            String realPath = BucketPath.getMeiTuanHadoopLogPath(path, event.getHeaders().get("category"), null);
            // filePrefix if fixed,  just use it
            String realName = fileName;
        }
        System.out.println("strFormat last : " + (System.currentTimeMillis() - t1));
    }

}
