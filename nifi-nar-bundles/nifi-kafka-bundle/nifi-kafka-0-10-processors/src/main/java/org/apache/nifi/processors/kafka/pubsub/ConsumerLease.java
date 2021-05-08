/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.kafka.pubsub;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_0_10.REL_PARSE_FAILURE;
import static org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_0_10.REL_SUCCESS;
import static org.apache.nifi.processors.kafka.pubsub.KafkaProcessorUtils.HEX_ENCODING;
import static org.apache.nifi.processors.kafka.pubsub.KafkaProcessorUtils.UTF8_ENCODING;

/**
 * This class represents a lease to access a Kafka Consumer object. The lease is
 * intended to be obtained from a ConsumerPool. The lease is closeable to allow
 * for the clean model of a try w/resources whereby non-exceptional cases mean
 * the lease will be returned to the pool for future use by others. A given
 * lease may only belong to a single thread a time.
 */
public abstract class ConsumerLease implements Closeable, ConsumerRebalanceListener {

    private final long maxWaitMillis;
    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final ComponentLog logger;
    private final byte[] demarcatorBytes;
    private final String keyEncoding;
    private final String securityProtocol;
    private final String bootstrapServers;
    private final RecordSetWriterFactory writerFactory;
    private final RecordReaderFactory readerFactory;
    private boolean poisoned = false;
    //used for tracking demarcated flowfiles to their TopicPartition so we can append
    //to them on subsequent poll calls
    private final Map<BundleInformation, BundleTracker> bundleMap = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> uncommittedOffsetsMap = new HashMap<>();
    private long leaseStartNanos = -1;
    private boolean lastPollEmpty = false;
    private int totalMessages = 0;

    ConsumerLease(
            final long maxWaitMillis,
            final Consumer<byte[], byte[]> kafkaConsumer,
            final byte[] demarcatorBytes,
            final String keyEncoding,
            final String securityProtocol,
            final String bootstrapServers,
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final ComponentLog logger) {
        this.maxWaitMillis = maxWaitMillis;
        this.kafkaConsumer = kafkaConsumer;
        this.demarcatorBytes = demarcatorBytes;
        this.keyEncoding = keyEncoding;
        this.securityProtocol = securityProtocol;
        this.bootstrapServers = bootstrapServers;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.logger = logger;
    }

    /**
     * clears out internal state elements excluding session and consumer as
     * those are managed by the pool itself
     */
    private void resetInternalState() {
        bundleMap.clear();
        uncommittedOffsetsMap.clear();
        leaseStartNanos = -1;
        lastPollEmpty = false;
        totalMessages = 0;
    }

    /**
     * Kafka will call this method whenever it is about to rebalance the
     * consumers for the given partitions. We'll simply take this to mean that
     * we need to quickly commit what we've got and will return the consumer to
     * the pool. This method will be called during the poll() method call of
     * this class and will be called by the same thread calling poll according
     * to the Kafka API docs. After this method executes the session and kafka
     * offsets are committed and this lease is closed.
     *
     * @param partitions partitions being reassigned
     */
    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        logger.debug("Rebalance Alert: Partitions '{}' revoked for lease '{}' with consumer '{}'", new Object[]{partitions, this, kafkaConsumer});
        //force a commit here.  Can reuse the session and consumer after this but must commit now to avoid duplicates if kafka reassigns partition
        commit();
    }

    /**
     * This will be called by Kafka when the rebalance has completed. We don't
     * need to do anything with this information other than optionally log it as
     * by this point we've committed what we've got and moved on.
     *
     * @param partitions topic partition set being reassigned
     */
    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        logger.debug("Rebalance Alert: Partitions '{}' assigned for lease '{}' with consumer '{}'", new Object[]{partitions, this, kafkaConsumer});
    }

    /**
     * Executes a poll on the underlying Kafka Consumer and creates any new
     * flowfiles necessary or appends to existing ones if in demarcation mode.
     */
    void poll() {
        /**
         * Implementation note:
         * Even if ConsumeKafka is not scheduled to poll due to downstream connection back-pressure is engaged,
         * for longer than session.timeout.ms (defaults to 10 sec), Kafka consumer sends heartbeat from background thread.
         * If this situation lasts longer than max.poll.interval.ms (defaults to 5 min), Kafka consumer sends
         * Leave Group request to Group Coordinator. When ConsumeKafka processor is scheduled again, Kafka client checks
         * if this client instance is still a part of consumer group. If not, it rejoins before polling messages.
         * This behavior has been fixed via Kafka KIP-62 and available from Kafka client 0.10.1.0.
         */
        try {
            final ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(10);
            lastPollEmpty = records.count() == 0;
            processRecords(records);
        } catch (final ProcessException pe) {
            throw pe;
        } catch (final Throwable t) {
            this.poison();
            throw t;
        }
    }

    /**
     * Notifies Kafka to commit the offsets for the specified topic/partition
     * pairs to the specified offsets w/the given metadata. This can offer
     * higher performance than the other commitOffsets call as it allows the
     * kafka client to collect more data from Kafka before committing the
     * offsets.
     *
     * if false then we didn't do anything and should probably yield if true
     * then we committed new data
     *
     */
    boolean commit() {
        if (uncommittedOffsetsMap.isEmpty()) {
            resetInternalState();
            return false;
        }
        try {
            /**
             * Committing the nifi session then the offsets means we have an at
             * least once guarantee here. If we reversed the order we'd have at
             * most once.
             */
            final Collection<FlowFile> bundledFlowFiles = getBundles();
            if (!bundledFlowFiles.isEmpty()) {
                getProcessSession().transfer(bundledFlowFiles, REL_SUCCESS);
            }
            getProcessSession().commit();

            final Map<TopicPartition, OffsetAndMetadata> offsetsMap = uncommittedOffsetsMap;
            kafkaConsumer.commitSync(offsetsMap);
            resetInternalState();
            return true;
        } catch (final IOException ioe) {
            poison();
            logger.error("Failed to finish writing out FlowFile bundle", ioe);
            throw new ProcessException(ioe);
        } catch (final KafkaException kex) {
            poison();
            logger.warn("Duplicates are likely as we were able to commit the process"
                    + " session but received an exception from Kafka while committing"
                    + " offsets.");
            throw kex;
        } catch (final Throwable t) {
            poison();
            throw t;
        }
    }

    /**
     * Indicates whether we should continue polling for data. If we are not
     * writing data with a demarcator then we're writing individual flow files
     * per kafka message therefore we must be very mindful of memory usage for
     * the flow file objects (not their content) being held in memory. The
     * content of kafka messages will be written to the content repository
     * immediately upon each poll call but we must still be mindful of how much
     * memory can be used in each poll call. We will indicate that we should
     * stop polling our last poll call produced no new results or if we've
     * polling and processing data longer than the specified maximum polling
     * time or if we have reached out specified max flow file limit or if a
     * rebalance has been initiated for one of the partitions we're watching;
     * otherwise true.
     *
     * @return true if should keep polling; false otherwise
     */
    boolean continuePolling() {
        //stop if the last poll produced new no data
        if (lastPollEmpty) {
            return false;
        }

        //stop if we've gone past our desired max uncommitted wait time
        if (leaseStartNanos < 0) {
            leaseStartNanos = System.nanoTime();
        }
        final long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - leaseStartNanos);
        if (durationMillis > maxWaitMillis) {
            return false;
        }

        //stop if we've generated enough flowfiles that we need to be concerned about memory usage for the objects
        if (bundleMap.size() > 200) { //a magic number - the number of simultaneous bundles to track
            return false;
        } else {
            return totalMessages < 1000;//admittedlly a magic number - good candidate for processor property
        }
    }

    /**
     * Indicates that the underlying session and consumer should be immediately
     * considered invalid. Once closed the session will be rolled back and the
     * pool should destroy the underlying consumer. This is useful if due to
     * external reasons, such as the processor no longer being scheduled, this
     * lease should be terminated immediately.
     */
    private void poison() {
        poisoned = true;
    }

    /**
     * @return true if this lease has been poisoned; false otherwise
     */
    boolean isPoisoned() {
        return poisoned;
    }

    /**
     * Trigger the consumer's {@link KafkaConsumer#wakeup() wakeup()} method.
     */
    public void wakeup() {
        kafkaConsumer.wakeup();
    }

    /**
     * Abstract method that is intended to be extended by the pool that created
     * this ConsumerLease object. It should ensure that the session given to
     * create this session is rolled back and that the underlying kafka consumer
     * is either returned to the pool for continued use or destroyed if this
     * lease has been poisoned. It can only be called once. Calling it more than
     * once can result in undefined and non threadsafe behavior.
     */
    @Override
    public void close() {
        resetInternalState();
    }

    public abstract ProcessSession getProcessSession();

    public abstract void yield();

    private void processRecords(final ConsumerRecords<byte[], byte[]> records) {
        records.partitions().stream().forEach(partition -> {
            List<ConsumerRecord<byte[], byte[]>> messages = records.records(partition);
            if (!messages.isEmpty()) {
                //update maximum offset map for this topic partition
                long maxOffset = messages.stream()
                        .mapToLong(record -> record.offset())
                        .max()
                        .getAsLong();

                //write records to content repository and session
                if (demarcatorBytes != null) {
                    writeDemarcatedData(getProcessSession(), messages, partition);
                } else if (readerFactory != null && writerFactory != null) {
                    writeRecordData(getProcessSession(), messages, partition);
                } else {
                    messages.stream().forEach(message -> {
                        writeData(getProcessSession(), message, partition);
                    });
                }

                totalMessages += messages.size();
                uncommittedOffsetsMap.put(partition, new OffsetAndMetadata(maxOffset + 1L));
            }
        });
    }

    private static String encodeKafkaKey(final byte[] key, final String encoding) {
        if (key == null) {
            return null;
        }

        if (HEX_ENCODING.getValue().equals(encoding)) {
            return DatatypeConverter.printHexBinary(key);
        } else if (UTF8_ENCODING.getValue().equals(encoding)) {
            return new String(key, StandardCharsets.UTF_8);
        } else {
            return null;  // won't happen because it is guaranteed by the Allowable Values
        }
    }

    private Collection<FlowFile> getBundles() throws IOException {
        final List<FlowFile> flowFiles = new ArrayList<>();
        for (final BundleTracker tracker : bundleMap.values()) {
            final boolean includeBundle = processBundle(tracker);
            if (includeBundle) {
                flowFiles.add(tracker.flowFile);
            }
        }
        return flowFiles;
    }

    private boolean processBundle(final BundleTracker bundle) throws IOException {
        final RecordSetWriter writer = bundle.recordWriter;
        if (writer != null) {
            final WriteResult writeResult;

            try {
                writeResult = writer.finishRecordSet();
            } finally {
                writer.close();
            }

            if (writeResult.getRecordCount() == 0) {
                getProcessSession().remove(bundle.flowFile);
                return false;
            }

            final Map<String, String> attributes = new HashMap<>();
            attributes.putAll(writeResult.getAttributes());
            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());

            bundle.flowFile = getProcessSession().putAllAttributes(bundle.flowFile, attributes);
        }

        populateAttributes(bundle);
        return true;
    }

    private void writeData(final ProcessSession session, ConsumerRecord<byte[], byte[]> record, final TopicPartition topicPartition) {
        FlowFile flowFile = session.create();
        final BundleTracker tracker = new BundleTracker(record, topicPartition, keyEncoding);
        tracker.incrementRecordCount(1);
        final byte[] value = record.value();
        if (value != null) {
            flowFile = session.write(flowFile, out -> {
                out.write(value);
            });
        }
        tracker.updateFlowFile(flowFile);
        populateAttributes(tracker);
        session.transfer(tracker.flowFile, REL_SUCCESS);
    }

    private void writeDemarcatedData(final ProcessSession session, final List<ConsumerRecord<byte[], byte[]>> records, final TopicPartition topicPartition) {
        final ConsumerRecord<byte[], byte[]> firstRecord = records.get(0);
        final boolean demarcateFirstRecord;
        final BundleInformation bundleInfo = new BundleInformation(topicPartition, null);
        BundleTracker tracker = bundleMap.get(bundleInfo);
        FlowFile flowFile;
        if (tracker == null) {
            tracker = new BundleTracker(firstRecord, topicPartition, keyEncoding);
            flowFile = session.create();
            tracker.updateFlowFile(flowFile);
            demarcateFirstRecord = false; //have not yet written records for this topic/partition in this lease
        } else {
            demarcateFirstRecord = true; //have already been writing records for this topic/partition in this lease
        }
        flowFile = tracker.flowFile;

        tracker.incrementRecordCount(records.size());
        flowFile = session.append(flowFile, out -> {
            boolean useDemarcator = demarcateFirstRecord;
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                if (useDemarcator) {
                    out.write(demarcatorBytes);
                }
                final byte[] value = record.value();
                if (value != null) {
                    out.write(record.value());
                }
                useDemarcator = true;
            }
        });
        tracker.updateFlowFile(flowFile);
        bundleMap.put(bundleInfo, tracker);
    }


    private void writeRecordData(final ProcessSession session, final List<ConsumerRecord<byte[], byte[]>> messages, final TopicPartition topicPartition) {
        RecordSetWriter writer = null;

        final BiConsumer<ConsumerRecord<byte[], byte[]>, Exception> handleParseFailure = (consumerRecord, e) -> {
            // If we are unable to parse the data, we need to transfer it to 'parse failure' relationship
            // And continue to the next message.
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(KafkaProcessorUtils.KAFKA_OFFSET, String.valueOf(consumerRecord.offset()));
            attributes.put(KafkaProcessorUtils.KAFKA_TIMESTAMP, String.valueOf(consumerRecord.timestamp()));
            attributes.put(KafkaProcessorUtils.KAFKA_PARTITION, String.valueOf(topicPartition.partition()));
            attributes.put(KafkaProcessorUtils.KAFKA_TOPIC, topicPartition.topic());

            FlowFile failureFlowFile = session.create();
            if (consumerRecord.value() != null) {
                failureFlowFile = session.write(failureFlowFile, out -> out.write(consumerRecord.value()));
            }
            failureFlowFile = session.putAllAttributes(failureFlowFile, attributes);

            final String transitUri = KafkaProcessorUtils.buildTransitURI(securityProtocol, bootstrapServers, topicPartition.topic());
            session.getProvenanceReporter().receive(failureFlowFile, transitUri);

            session.transfer(failureFlowFile, REL_PARSE_FAILURE);
            logger.error("Failed to parse message from Kafka using the configured Record Reader. "
                    + "Will route message as its own FlowFile to the 'parse.failure' relationship", e);

            session.adjustCounter("Parse Failures", 1, false);
        };

        try {
            for (final ConsumerRecord<byte[], byte[]> consumerRecord : messages) {
                final byte[] recordBytes = consumerRecord.value() == null ? new byte[0] : consumerRecord.value();
                try (final InputStream in = new ByteArrayInputStream(recordBytes)) {

                    final RecordReader reader;
                    final Record firstRecord;

                    try {
                        reader = readerFactory.createRecordReader(Collections.emptyMap(), in, recordBytes.length, logger);
                        firstRecord = reader.nextRecord();
                    } catch (final Exception e) {
                        handleParseFailure.accept(consumerRecord, e);
                        continue;
                    }

                    if (firstRecord == null) {
                        // If the message doesn't contain any record, do nothing.
                        continue;
                    }

                    // Determine the bundle for this record.
                    final RecordSchema recordSchema = firstRecord.getSchema();
                    final BundleInformation bundleInfo = new BundleInformation(topicPartition, recordSchema);

                    BundleTracker tracker = bundleMap.get(bundleInfo);
                    if (tracker == null) {
                        FlowFile flowFile = session.create();
                        final OutputStream rawOut = session.write(flowFile);

                        final RecordSchema writeSchema;
                        try {
                            writeSchema = writerFactory.getSchema(Collections.emptyMap(), recordSchema);
                        } catch (final Exception e) {
                            logger.error("Failed to obtain Schema for FlowFile. Will roll back the Kafka message offsets.", e);

                            try {
                                rollback(topicPartition);
                            } catch (final Exception rollbackException) {
                                logger.warn("Attempted to rollback Kafka message offset but was unable to do so", rollbackException);
                            }

                            yield();
                            throw new ProcessException(e);
                        }

                        writer = writerFactory.createWriter(logger, writeSchema, rawOut, flowFile);
                        writer.beginRecordSet();

                        tracker = new BundleTracker(consumerRecord, topicPartition, keyEncoding, writer);
                        tracker.updateFlowFile(flowFile);
                        bundleMap.put(bundleInfo, tracker);
                    } else {
                        writer = tracker.recordWriter;
                    }

                    try {
                        for (Record record = firstRecord; record != null; record = reader.nextRecord()) {
                            writer.write(record);
                            tracker.incrementRecordCount(1L);
                            session.adjustCounter("Records Received", 1, false);
                        }
                    } catch (Exception e) {
                        // Transfer it to 'parse failure' and continue to the next message.
                        handleParseFailure.accept(consumerRecord, e);
                    }
                }
            }
        } catch (final Exception e) {
            logger.error("Failed to properly receive messages from Kafka. Will roll back session and any un-committed offsets from Kafka.", e);

            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (final Exception ioe) {
                logger.warn("Failed to close Record Writer", ioe);
            }

            try {
                rollback(topicPartition);
            } catch (final Exception rollbackException) {
                logger.warn("Attempted to rollback Kafka message offset but was unable to do so", rollbackException);
            }

            throw new ProcessException(e);
        }
    }


    private void rollback(final TopicPartition topicPartition) {
        OffsetAndMetadata offsetAndMetadata = uncommittedOffsetsMap.get(topicPartition);
        if (offsetAndMetadata == null) {
            offsetAndMetadata = kafkaConsumer.committed(topicPartition);
        }

        final long offset = offsetAndMetadata.offset();
        kafkaConsumer.seek(topicPartition, offset);
    }



    private void populateAttributes(final BundleTracker tracker) {
        final Map<String, String> kafkaAttrs = new HashMap<>();
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_OFFSET, String.valueOf(tracker.initialOffset));
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_TIMESTAMP, String.valueOf(tracker.initialTimestamp));
        if (tracker.key != null && tracker.totalRecords == 1) {
            kafkaAttrs.put(KafkaProcessorUtils.KAFKA_KEY, tracker.key);
        }
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_PARTITION, String.valueOf(tracker.partition));
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_TOPIC, tracker.topic);
        if (tracker.totalRecords > 1) {
            // Add a record.count attribute to remain consistent with other record-oriented processors. If not
            // reading/writing records, then use "kafka.count" attribute.
            if (tracker.recordWriter == null) {
                kafkaAttrs.put(KafkaProcessorUtils.KAFKA_COUNT, String.valueOf(tracker.totalRecords));
            } else {
                kafkaAttrs.put("record.count", String.valueOf(tracker.totalRecords));
            }
        }
        final FlowFile newFlowFile = getProcessSession().putAllAttributes(tracker.flowFile, kafkaAttrs);
        final long executionDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - leaseStartNanos);
        final String transitUri = KafkaProcessorUtils.buildTransitURI(securityProtocol, bootstrapServers, tracker.topic);
        getProcessSession().getProvenanceReporter().receive(newFlowFile, transitUri, executionDurationMillis);
        tracker.updateFlowFile(newFlowFile);
    }

    private static class BundleTracker {

        final long initialOffset;
        final long initialTimestamp;
        final int partition;
        final String topic;
        final String key;
        final RecordSetWriter recordWriter;
        FlowFile flowFile;
        long totalRecords = 0;

        private BundleTracker(final ConsumerRecord<byte[], byte[]> initialRecord, final TopicPartition topicPartition, final String keyEncoding) {
            this(initialRecord, topicPartition, keyEncoding, null);
        }

        private BundleTracker(final ConsumerRecord<byte[], byte[]> initialRecord, final TopicPartition topicPartition, final String keyEncoding, final RecordSetWriter recordWriter) {
            this.initialOffset = initialRecord.offset();
            this.initialTimestamp = initialRecord.timestamp();
            this.partition = topicPartition.partition();
            this.topic = topicPartition.topic();
            this.recordWriter = recordWriter;
            this.key = encodeKafkaKey(initialRecord.key(), keyEncoding);
        }

        private void incrementRecordCount(final long count) {
            totalRecords += count;
        }

        private void updateFlowFile(final FlowFile flowFile) {
            this.flowFile = flowFile;
        }

    }

    private static class BundleInformation {
        private final TopicPartition topicPartition;
        private final RecordSchema schema;

        public BundleInformation(final TopicPartition topicPartition, final RecordSchema schema) {
            this.topicPartition = topicPartition;
            this.schema = schema;
        }

        @Override
        public int hashCode() {
            return 41 + 13 * topicPartition.hashCode() + ((schema == null) ? 0 : 13 * schema.hashCode());
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof BundleInformation)) {
                return false;
            }
            final BundleInformation other = (BundleInformation) obj;
            return Objects.equals(topicPartition, other.topicPartition) && Objects.equals(schema, other.schema);
        }
    }
}
