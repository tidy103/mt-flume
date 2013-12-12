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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Shangan Chen, refer to Kees Jan Koster
 */
public class ZabbixSender {

    private static final Logger logger = LoggerFactory.getLogger(ZabbixSender.class);

    private final List<ZabbixItem> list;

    private final Map<String, Integer> zabbixServers;

    private final String head;

    private final String host;

    private static final String middle = "</key><data>";

    private static final String tail = "</data></req>";

    private final byte[] response = new byte[1024];

    private boolean stopping = false;

    private static final int retryNumber = 10;

    private static final int TIMEOUT = 30 * 1000;

    /**
     * Create a new background sender.
     * 
     * @param list
     *            The list to get data items from.
     * @param zabbixServer
     *            The name or IP of the machine to send the data to.
     * @param zabbixPort
     *            The port number on that machine.
     * @param host
     *            The host name, as defined in the host definition in Zabbix.
     * 
     */
    public ZabbixSender(List<ZabbixItem> list,
            Map<String, Integer> ZabbixServers,

            final String host) {
        /*
         * super("Zabbix-sender"); setDaemon(true);
         */
        this.list = list;
        this.zabbixServers = ZabbixServers;
        this.host = host;
        this.head = "<req><host>" + base64Encode(host) + "</host><key>";
    }

    /**
     * Indicate that we are about to stop.
     */
    public void stopping() {
        stopping = true;
        /* interrupt(); */
    }

    public void send() {
        
        logger.trace("begin send");
        try {
            for (ZabbixItem item : list) {
                int retryCount = 0;
                while (retryCount <= retryNumber) {
                    try {
                        send(item.getKey(), item.getValue());
                        break;
                    } catch (Exception e) {
                        logger.warn("Warning while sending item "
                                + item.getKey() + " value " + item.getValue()
                                + " on host " + host + " retry number "
                                + retryCount + " error:" + e);
                        Thread.sleep(1000);
                        retryCount++;
                        if (retryCount == retryNumber) {
                            logger.warn("Error i didn't sent item "
                                    + item.getKey() + " on Zabbix server "
                                    + " on host " + host + " tried "
                                    + retryCount + " times");
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            if (!stopping) {
                logger.warn("ignoring exception", e);
            }

        } catch (Exception e) {
            logger.warn("ignoring exception", e);
        }
        
        logger.trace("finished");
    }

    /**
     * Encodes data for transmission to the server.
     * 
     * This method encodes the data in the ASCII encoding, defaulting to the
     * platform default encoding if that is somehow unavailable.
     * 
     * @param data
     * @return byte[] containing the encoded data
     */
    protected byte[] encodeString(String data) {
        try {
            return data.getBytes("ASCII");
        } catch (UnsupportedEncodingException e) {
            return data.getBytes();
        }
    }

    protected String base64Encode(String data) {
        return new String(Base64.encodeBase64(encodeString(data)));
    }

    private void send(final String key, final String value) throws IOException {
        final StringBuilder message = new StringBuilder(head);
        // message.append(Base64.encode(key));
        message.append(base64Encode(key));
        message.append(middle);
        // message.append(Base64.encode(value == null ? "" : value));
        message.append(base64Encode(value == null ? "" : value));
        message.append(tail);

        logger.trace("sending " + message);

        Socket zabbix = null;
        OutputStreamWriter out = null;
        InputStream in = null;

        for(Map.Entry<String, Integer> zabbixServer : zabbixServers.entrySet()){
          
            try {
                
                zabbix = new Socket(zabbixServer.getKey(),  zabbixServer.getValue());
                zabbix.setSoTimeout(TIMEOUT);

                out = new OutputStreamWriter(zabbix.getOutputStream());
                out.write(message.toString());
                out.flush();

                in = zabbix.getInputStream();
                final int read = in.read(response);
                logger.trace("received " + new String(response));
                if (read != 2 || response[0] != 'O' || response[1] != 'K') {
                    logger.warn("received unexpected response '"
                            + new String(response) + "' for key '" + key + "'");
                }
            } catch (Exception ex) {
                logger.warn("Error contacting Zabbix server " + zabbixServer.getKey()
                        + "  on port " + zabbixServer.getValue());
            }

            finally {
                if (in != null) {
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
                if (zabbix != null) {
                    zabbix.close();
                }

            }
        }
    }
}
