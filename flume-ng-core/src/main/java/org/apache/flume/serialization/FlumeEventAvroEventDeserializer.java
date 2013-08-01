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
package org.apache.flume.serialization;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.HashMap;
import java.util.Date;
import java.nio.ByteBuffer;

/**
 * A deserializer that parses Avro container files, generating one Flume event
 * per record in the Avro file, and storing binary avro-encoded records in
 * the Flume event body.
 */
public class FlumeEventAvroEventDeserializer implements EventDeserializer {

  private static final Logger logger = LoggerFactory.getLogger
      (FlumeEventAvroEventDeserializer.class);

  private final ResettableInputStream ris;
  private Schema schema;
  private DataFileReader<GenericRecord> fileReader;
  private GenericRecord record;

  private FlumeEventAvroEventDeserializer(Context context, ResettableInputStream ris) {
    this.ris = ris;
  }

  private void initialize() throws IOException, NoSuchAlgorithmException {
    SeekableResettableInputBridge in = new SeekableResettableInputBridge(ris);
    long pos = in.tell();
    in.seek(0L);
    fileReader = new DataFileReader<GenericRecord>(in,
        new GenericDatumReader<GenericRecord>());
    fileReader.sync(pos);

    schema = fileReader.getSchema();
  }

  @Override
  public Event readEvent() throws IOException {
    if (fileReader.hasNext()) {
      record = fileReader.next(record);

      HashMap<String, String> newHeaders = new HashMap<String, String>();
      HashMap<Utf8, Utf8> headers = (HashMap<Utf8, Utf8>)record.get("headers");
      newHeaders.put("timestamp", Long.valueOf((new Date().getTime())).toString());
      newHeaders.put("category", headers.get(new Utf8("category")).toString());

      ByteBuffer body = (ByteBuffer)record.get("body");
      int length = body.limit() - body.position();
      byte[] bodyBytes = new byte[length];
      System.arraycopy(body.array(), body.position(), bodyBytes, 0, length);

      return EventBuilder.withBody(bodyBytes, newHeaders);
    }
    return null;
  }

  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    List<Event> events = Lists.newArrayList();
    for (int i = 0; i < numEvents && fileReader.hasNext(); i++) {
      Event event = readEvent();
      if (event != null) {
        events.add(event);
      }
    }
    return events;
  }

  @Override
  public void mark() throws IOException {
    long pos = fileReader.previousSync() - DataFileConstants.SYNC_SIZE;
    if (pos < 0) pos = 0;
    ((RemoteMarkable) ris).markPosition(pos);
  }

  @Override
  public void reset() throws IOException {
    long pos = ((RemoteMarkable) ris).getMarkPosition();
    fileReader.sync(pos);
  }

  @Override
  public void close() throws IOException {
    ris.close();
  }

  public static class Builder implements EventDeserializer.Builder {

    @Override
    public EventDeserializer build(Context context, ResettableInputStream in) {
      if (!(in instanceof RemoteMarkable)) {
        throw new IllegalArgumentException("Cannot use this deserializer " +
            "without a RemoteMarkable input stream");
      }
      FlumeEventAvroEventDeserializer deserializer
          = new FlumeEventAvroEventDeserializer(context, in);
      try {
        deserializer.initialize();
      } catch (Exception e) {
        throw new FlumeException("Cannot instantiate deserializer", e);
      }
      return deserializer;
    }
  }

  private static class SeekableResettableInputBridge implements SeekableInput {

    ResettableInputStream ris;
    public SeekableResettableInputBridge(ResettableInputStream ris) {
      this.ris = ris;
    }

    @Override
    public void seek(long p) throws IOException {
      ris.seek(p);
    }

    @Override
    public long tell() throws IOException {
      return ris.tell();
    }

    @Override
    public long length() throws IOException {
      if (ris instanceof LengthMeasurable) {
        return ((LengthMeasurable) ris).length();
      } else {
        // FIXME: Avro doesn't seem to complain about this,
        // but probably not a great idea...
        return Long.MAX_VALUE;
      }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return ris.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
      ris.close();
    }
  }

}
