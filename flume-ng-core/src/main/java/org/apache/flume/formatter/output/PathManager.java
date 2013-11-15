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

package org.apache.flume.formatter.output;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PathManager {

  //private long seriesTimestamp;
  private File baseDirectory;
  private String filePrefix;
  private SimpleDateFormat sdf;
  //private AtomicInteger fileIndex;

  private File currentFile;

  public PathManager() {
    //seriesTimestamp = System.currentTimeMillis();
    //fileIndex = new AtomicInteger();
	sdf = new SimpleDateFormat("yyyy-MM-dd_000HH");
  }

  public File nextFile() {
	String curDateHourStr = sdf.format(new Date(System.currentTimeMillis()));
	String fileName = filePrefix + "-" + curDateHourStr;  
    currentFile = new File(baseDirectory, fileName);

    return currentFile;
  }

  public File getCurrentFile() {
    if (currentFile == null) {
      return nextFile();
    }

    return currentFile;
  }
  
  public void createCurrentSymbolicFile() throws IOException {
	if (currentFile != null) {
		String target = currentFile.getAbsolutePath();
		File newLinkFile = new File(baseDirectory, filePrefix + "_current");
		String newLink = newLinkFile.getAbsolutePath();
		
		//1. delete
		if (newLinkFile.exists()) {
			newLinkFile.delete();
		}
		//2. create new
		ProcessBuilder pb = new ProcessBuilder("/bin/ln", "-s", target, newLink); 
		Process proc = pb.start();
        try {
            int exitValue = proc.waitFor();
    		if (exitValue != 0) {
    			throw new IOException("create symbolic file error.");
    		}
        } catch (InterruptedException e) {
        }
	}
  }

  public void rotate() {
    currentFile = null;
  }

  public File getBaseDirectory() {
    return baseDirectory;
  }

  public void setBaseDirectory(File baseDirectory) {
    this.baseDirectory = baseDirectory;
  }

  public String getFilePrefix() {
	return filePrefix;
  }
	
  public void setFilePrefix(String filePrefix) {
	this.filePrefix = filePrefix;
  }

}
