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
package org.apache.flume.sink.hdfs;

public class HDFSEventSinkMetric {
    
    private String category;
    private int eventNum;
    private long all;
    private long take;
    private long append;
    private long sync;
    
    public HDFSEventSinkMetric(){
        this.eventNum = 0;
        this.all = 0;
        this.take = 0;
        this.append = 0;
        this.sync = 0;
    }    
    public HDFSEventSinkMetric(String category){
        this();
        this.category = category;
    }
    public String getCategory() {
        return category;
    }
    public void setCategory(String category) {
        this.category = category;
    }
    
    public int getEventNum() {
        return eventNum;
    }
    public void setEventNum(int eventNum) {
        this.eventNum = eventNum;
    }
    public void incEventNum(int inc){
        this.eventNum += inc;
    }
    public long getAll() {
        return all;
    }
    public void setAll(long all) {
        this.all = all;
    }
    public void incAll(long inc){
        this.all += inc;
    }
    public long getTake() {
        return take;
    }
    public void setTake(long take) {
        this.take = take;
    }
    public void incTake(long inc){
        this.take += inc;
    }
    public long getAppend() {
        return append;
    }
    public void setAppend(long append) {
        this.append = append;
    }
    public void incAppend(long inc){
        this.append += inc;
    }
    public long getSync() {
        return sync;
    }
    public void setSync(long sync) {
        this.sync = sync;
    }
    public void incSync(long inc){
        this.sync += inc;
    }
    
    public void sum(){
        this.all = this.take + this.append + this.sync;
    }
    @Override
    public String toString() {
        
        return String.format("HDFSEventSinkMetric category=%s, eventNum[%d] all[%d] take[%d]" +
        		" append[%d] sync[%d]", category, eventNum, all, take, append, sync);
    }
    
}
