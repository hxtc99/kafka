/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.WindowStoreUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RocksDBWindowStore<K, V> implements WindowStore<K, V> {

    private static final Logger log = LoggerFactory.getLogger(RocksDBWindowStore.class);

    public static final long MIN_SEGMENT_INTERVAL = 60 * 1000; // one minute

    private static final long USE_CURRENT_TIMESTAMP = -1L;

    private static final int IN_MEMORY_JOIN_ENTRIES = 1500 * 1000;
    private static final int IN_MEMORY_AGG_ENTRIES = Integer.MAX_VALUE;


    private enum CompressionStyle {
        NONE,
        GZIP,
        SNAPPY,
    }
    private static final CompressionStyle COMPRESSION_STYLE = CompressionStyle.SNAPPY;

    private static InputStream getCompressedInputStream(InputStream inner) throws IOException {
        if (COMPRESSION_STYLE == CompressionStyle.GZIP) {
            return new GZIPInputStream(inner);
        } else if (COMPRESSION_STYLE == CompressionStyle.SNAPPY) {
            return new SnappyInputStream(inner);
        }
        throw new RuntimeException("Unknown compression style: " + COMPRESSION_STYLE);
    }

    private static OutputStream getCompressedOutputStream(OutputStream inner) throws IOException {
        if (COMPRESSION_STYLE == CompressionStyle.GZIP) {
            return new GZIPOutputStream(inner);
        } else if (COMPRESSION_STYLE == CompressionStyle.SNAPPY) {
            return new SnappyOutputStream(inner);
        }
        throw new RuntimeException("Unknown compression style: " + COMPRESSION_STYLE);
    }

    private static byte[] compress(byte[] value) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(value.length);
            OutputStream gos = getCompressedOutputStream(bos);
            gos.write(value);
            gos.flush();
            gos.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException ex) {
            throw new RuntimeException("Problem in compression.", ex);
        }
    }

    private static final byte[] uncompress(byte[] value) {
        int bufSize = value.length * 4;
        byte[] bytes = new byte[bufSize];
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(value);
            InputStream gis = getCompressedInputStream(bis);
            ByteArrayOutputStream bos = new ByteArrayOutputStream(bufSize);
            int len;
            while ((len=gis.read(bytes, 0, bufSize)) != -1) {
                bos.write(bytes, 0, len);
            }
            byte[] output = bos.toByteArray();
            gis.close();
            bis.close();
            bos.close();
            return output;
        } catch (IOException ex) {
            throw new RuntimeException("Problem in decompression.", ex);
        }
    }

    private interface Segment extends KeyValueStore<byte[],byte[]> {
        long id();
        void destroy();
        void openDB(ProcessorContext context);
        int cacheSize();
    }

    private static class RocksDBSegment extends RocksDBStore<byte[], byte[]>
        implements Segment {
        public final long id;

        RocksDBSegment(String segmentName, String windowName, long id) {
            super(segmentName, windowName, WindowStoreUtils.INNER_SERDE, WindowStoreUtils.INNER_SERDE);
            this.id = id;
        }

        public long id() {return id;}
        public void destroy() {Utils.delete(dbDir);}
        public void openDB(ProcessorContext context) {super.openDB(context);}
    }

    private static class InMemorySegment extends MemoryNavigableLRUCache<byte[], byte[]>
        implements Segment {

        private final boolean usingCompression;
        public final long id;
        private final int maxEntries;
        private int elementsRemoved = 0;
        private int loggingThreashold = 1;

        InMemorySegment(String segmentName, int maxEntries, long id, boolean usingCompression) {
            super(segmentName, maxEntries, new RawStoreChangeLogger.ByteArrayComparator());
            this.id = id;
            this.maxEntries = maxEntries;
            this.usingCompression = usingCompression;
            log.info("Cache size {} for segment: {}, id: {}, compression: {}",
                maxEntries, segmentName, id, usingCompression);
            whenEldestRemoved(
                new MemoryNavigableLRUCache.EldestEntryRemovalListener<byte[], byte[]>() {
                    @Override
                    public void apply(byte[] key, byte[] value) {
                        ++elementsRemoved;
                        if (elementsRemoved >= loggingThreashold) {
                            log.warn("Segment: {}, eldest removal: {}", name, elementsRemoved);
                            loggingThreashold *= 2;
                        }
                    }
                });
        }

        public long id() {return id;}
        public void destroy() {
            log.info("Destroy: {}, id: {}", name(), id);
            keys.clear();
            map.clear();
        }
        public void openDB(ProcessorContext context) {}
        public int cacheSize() {return map.size();}

        @Override
        public byte[] get(byte[] key) {
            byte[] value = super.get(key);
            if (usingCompression) {
                value = uncompress(value);
            }
            return value;
        }

        @Override
        public void put(byte[] key, byte[] value) {
            if (usingCompression) {
                value = compress(value);
            }
            super.put(key, value);
        }

    }

    private static class RocksDBWindowStoreIterator<V> implements WindowStoreIterator<V> {
        private final StateSerdes<?, V> serdes;
        private final KeyValueIterator<byte[], byte[]>[] iterators;
        private final boolean usingCompression;
        private int index = 0;

        RocksDBWindowStoreIterator(StateSerdes<?, V> serdes, boolean usingCompression) {
            this(serdes, usingCompression, WindowStoreUtils.NO_ITERATORS);
        }

        RocksDBWindowStoreIterator(StateSerdes<?, V> serdes, boolean usingCompression,
                                   KeyValueIterator<byte[], byte[]>[] iterators) {
            this.serdes = serdes;
            this.iterators = iterators;
            this.usingCompression = usingCompression;
        }

        @Override
        public boolean hasNext() {
            while (index < iterators.length) {
                if (iterators[index].hasNext())
                    return true;

                index++;
            }
            return false;
        }

        /**
         * @throws NoSuchElementException if no next element exists
         */
        @Override
        public KeyValue<Long, V> next() {
            if (index >= iterators.length)
                throw new NoSuchElementException();

            KeyValue<byte[], byte[]> kv = iterators[index].next();
            if (usingCompression) {
                kv = new KeyValue<>(kv.key, uncompress(kv.value));
            }

            return new KeyValue<>(WindowStoreUtils.timestampFromBinaryKey(kv.key),
                                  serdes.valueFrom(kv.value));
        }

        @Override
        public void remove() {
            if (index < iterators.length)
                iterators[index].remove();
        }

        @Override
        public void close() {
            for (KeyValueIterator<byte[], byte[]> iterator : iterators) {
                iterator.close();
            }
        }
    }

    private final String name;
    private final long retentionPeriod;
    private final long segmentInterval;
    private final int numSegments;
    private final boolean retainDuplicates;
    private final Segment[] segments;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final SimpleDateFormat formatter;
    private final StoreChangeLogger.ValueGetter<byte[], byte[]> getter;
    private final boolean inMemory;

    private ProcessorContext context;
    private int seqnum = 0;
    private long currentSegmentId = -1L;

    private StateSerdes<K, V> serdes;

    private boolean loggingEnabled = false;
    private StoreChangeLogger<byte[], byte[]> changeLogger = null;
    private boolean usingCompression = false;
    private final int inMemoryEntries;

    public RocksDBWindowStore(String name, long retentionPeriod, int numSegments, boolean retainDuplicates, Serde<K> keySerde, Serde<V> valueSerde) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.numSegments = numSegments;

        this.inMemory = name.contains("in-memory");
        if (inMemory) {
            usingCompression = true;
        }
        inMemoryEntries = name.contains("join") ? IN_MEMORY_JOIN_ENTRIES : IN_MEMORY_AGG_ENTRIES;

        // The segment interval must be greater than MIN_SEGMENT_INTERVAL
        this.segmentInterval = Math.max(retentionPeriod / (numSegments - 1), MIN_SEGMENT_INTERVAL);
        log.info("name: {}, in-memory: {}, numSegments: {}, segmentInterval: {}, compression: {}, style: {}",
            name, inMemory, numSegments, segmentInterval, usingCompression, COMPRESSION_STYLE);

        this.segments = new Segment[numSegments];
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;

        this.retainDuplicates = retainDuplicates;

        this.getter = new StoreChangeLogger.ValueGetter<byte[], byte[]>() {
            public byte[] get(byte[] key) {
                return getInternal(key);
            }
        };

        // Create a date formatter. Formatted timestamps are used as segment name suffixes
        this.formatter = new SimpleDateFormat("yyyyMMddHHmm");
        this.formatter.setTimeZone(new SimpleTimeZone(0, "GMT"));
    }

    public RocksDBWindowStore<K, V> enableLogging() {
        loggingEnabled = true;

        return this;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context, StateStore root) {
        this.context = context;

        // construct the serde
        this.serdes = new StateSerdes<>(name,
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        if (!inMemory) {
            openExistingSegments();
        }

        this.changeLogger = this.loggingEnabled ? new RawStoreChangeLogger(name, context) : null;

        // register and possibly restore the state from the logs
        context.register(root, loggingEnabled, new StateRestoreCallback() {
            @Override
            public void restore(byte[] key, byte[] value) {
                putInternal(key, value);
            }
        });

        flush();
    }

    private void openExistingSegments() {
        try {
            File dir = new File(context.stateDir(), name);

            if (dir.exists()) {
                String[] list = dir.list();
                if (list != null) {
                    long[] segmentIds = new long[list.length];
                    for (int i = 0; i < list.length; i++)
                        segmentIds[i] = segmentIdFromSegmentName(list[i]);

                    // open segments in the id order
                    Arrays.sort(segmentIds);
                    for (long segmentId : segmentIds) {
                        if (segmentId >= 0) {
                            currentSegmentId = segmentId;
                            getSegment(segmentId);
                        }
                    }
                }
            } else {
                dir.mkdir();
            }
        } catch (Exception ex) {
            // ignore
        }
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public void flush() {
        for (Segment segment : segments) {
            if (segment != null)
                segment.flush();
        }

        if (loggingEnabled)
            changeLogger.logChange(this.getter);
    }

    @Override
    public void close() {
        log.info("close store: {}", name);
        flush();
        for (Segment segment : segments) {
            if (segment != null)
                segment.close();
        }
    }

    @Override
    public void put(K key, V value) {
        byte[] rawKey = putAndReturnInternalKey(key, value, USE_CURRENT_TIMESTAMP);

        if (rawKey != null && loggingEnabled) {
            changeLogger.add(rawKey);
            changeLogger.maybeLogChange(this.getter);
        }
    }

    @Override
    public void put(K key, V value, long timestamp) {
        byte[] rawKey = putAndReturnInternalKey(key, value, timestamp);

        if (rawKey != null && loggingEnabled) {
            changeLogger.add(rawKey);
            changeLogger.maybeLogChange(this.getter);
        }
    }

    private byte[] putAndReturnInternalKey(K key, V value, long t) {
        long timestamp = t == USE_CURRENT_TIMESTAMP ? context.timestamp() : t;

        long segmentId = segmentId(timestamp);

        if (segmentId > currentSegmentId) {
            // A new segment will be created. Clean up old segments first.
            currentSegmentId = segmentId;
            cleanup();
        }

        // If the record is within the retention period, put it in the store.
        Segment segment = getSegment(segmentId);
        if (segment != null) {
            if (retainDuplicates)
                seqnum = (seqnum + 1) & 0x7FFFFFFF;
            byte[] binaryKey = WindowStoreUtils.toBinaryKey(key, timestamp, seqnum, serdes);
            segment.put(binaryKey, serdes.rawValue(value));
            maybeCloseOldest();
            return binaryKey;
        } else {
            return null;
        }
    }

    private void putInternal(byte[] binaryKey, byte[] binaryValue) {
        long segmentId = segmentId(WindowStoreUtils.timestampFromBinaryKey(binaryKey));

        if (segmentId > currentSegmentId) {
            // A new segment will be created. Clean up old segments first.
            currentSegmentId = segmentId;
            cleanup();
        }

        // If the record is within the retention period, put it in the store.
        Segment segment = getSegment(segmentId);
        if (segment != null) {
            segment.put(binaryKey, binaryValue);
            maybeCloseOldest();
        }
    }

    private int totalSize() {
        int size = 0;
        for (int i = 0; i < segments.length; i++) {
            if (segments[i] != null) {
                size += segments[i].cacheSize();
            }
        }
        return size;
    }

    private byte[] getInternal(byte[] binaryKey) {
        long segmentId = segmentId(WindowStoreUtils.timestampFromBinaryKey(binaryKey));

        Segment segment = getSegment(segmentId);
        if (segment != null) {
            return segment.get(binaryKey);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public WindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo) {
        long segFrom = segmentId(timeFrom);
        long segTo = segmentId(Math.max(0L, timeTo));

        byte[] binaryFrom = WindowStoreUtils.toBinaryKey(key, timeFrom, 0, serdes);
        byte[] binaryTo = WindowStoreUtils.toBinaryKey(key, timeTo, Integer.MAX_VALUE, serdes);

        ArrayList<KeyValueIterator<byte[], byte[]>> iterators = new ArrayList<>();

        for (long segmentId = segFrom; segmentId <= segTo; segmentId++) {
            Segment segment = getSegment(segmentId);
            if (segment != null)
                iterators.add(segment.range(binaryFrom, binaryTo));
        }

        if (iterators.size() > 0) {
            return new RocksDBWindowStoreIterator<>(serdes, usingCompression, iterators.toArray(new KeyValueIterator[iterators.size()]));
        } else {
            return new RocksDBWindowStoreIterator<>(serdes, usingCompression);
        }
    }

    private Segment getSegment(long segmentId) {
        if (segmentId <= currentSegmentId && segmentId > currentSegmentId - segments.length) {
            int index = (int) (segmentId % segments.length);

            if (segments[index] != null && segments[index].id() != segmentId) {
                cleanup();
            }

            if (segments[index] == null) {
                segments[index] = inMemory
                                  ? new InMemorySegment(segmentName(segmentId),
                                      inMemoryEntries / 2, segmentId, usingCompression)
                                  : new RocksDBSegment(segmentName(segmentId), name, segmentId);
                segments[index].openDB(context);
            }

            return segments[index];

        } else {
            return null;
        }
    }

    private void cleanup() {
        for (int i = 0; i < segments.length; i++) {
            if (segments[i] != null && segments[i].id() <= currentSegmentId - segments.length) {
                log.info("cleanup: name: {}, currentSegment: {}, length: {}, segment: {}",
                    name, currentSegmentId, segments.length, segments[i].id());
                segments[i].close();
                segments[i].destroy();
                segments[i] = null;
            }
        }
    }

    private void maybeCloseOldest() {
        int size = totalSize();
        if (!inMemory || size <= inMemoryEntries) return;

        log.warn("Too many entries: {}, closing oldest segment", size);

        int index = (int) (currentSegmentId + 1) % segments.length;
        for (int i = 0; i < segments.length; i++) {
            if (segments[index] != null) {
                log.warn("Close oldest segment: {} due to memory pressure", segments[index].name());
                segments[index].close();
                segments[index].destroy();
                segments[index] = null;
                return;
            }
            index = (index + 1) % segments.length;
        }
        log.warn("Unable to find a segment to close");
    }

    private long segmentId(long timestamp) {
        return timestamp / segmentInterval;
    }

    // this method is defined public since it is used for unit tests
    public String segmentName(long segmentId) {
        return formatter.format(new Date(segmentId * segmentInterval));
    }

    public long segmentIdFromSegmentName(String segmentName) {
        try {
            Date date = formatter.parse(segmentName);
            return date.getTime() / segmentInterval;
        } catch (Exception ex) {
            return -1L;
        }
    }

    // this method is defined public since it is used for unit tests
    public Set<Long> segmentIds() {
        HashSet<Long> segmentIds = new HashSet<>();

        for (Segment segment : segments) {
            if (segment != null)
                segmentIds.add(segment.id());
        }

        return segmentIds;
    }

}
