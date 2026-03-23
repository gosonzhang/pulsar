/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for decode failure scenarios in
 * {@link ConsumerImpl#receiveIndividualMessagesFromBatch}.
 *
 */
public class BatchMessageDecodeFailureTest {

    private static final String TOPIC = "persistent://tenant/ns1/test-decode-failure";

    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;
    private ClientCnx mockCnx;
    private ConsumerImpl<byte[]> consumer;
    private ConsumerStatsRecorderImpl statsRecorder;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        executorProvider = new ExecutorProvider(1, "BatchDecodeFailureTest");
        internalExecutor = Executors.newSingleThreadScheduledExecutor();

        mockCnx = ClientTestFixtures.mockClientCnx();

        PulsarClientImpl client = ClientTestFixtures.createPulsarClientMockWithMockedClientCnx(
                executorProvider, internalExecutor, mockCnx);
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        // Set StatsIntervalSeconds > 0 to enable real stats recording
        clientConf.setStatsIntervalSeconds(1);

        ConsumerConfigurationData<byte[]> consumerConf = new ConsumerConfigurationData<>();
        consumerConf.setSubscriptionName("test-sub");

        CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();
        consumer = ConsumerImpl.newConsumerImpl(client, TOPIC, consumerConf,
                executorProvider, -1, false, subscribeFuture, null, null, null,
                true);
        consumer.setState(HandlerState.State.Ready);
        consumer.setClientCnx(mockCnx);

        // Replace the stats field with a spy to verify incrementNumReceiveFailed calls.
        // Use FieldUtils.writeField to inject the spy into the final field.
        statsRecorder = spy(new ConsumerStatsRecorderImpl(consumer));
        try {
            FieldUtils.writeField(consumer, "stats", statsRecorder, true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject spy stats recorder", e);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (executorProvider != null) {
            executorProvider.shutdownNow();
            executorProvider = null;
        }
        if (internalExecutor != null) {
            internalExecutor.shutdownNow();
            internalExecutor = null;
        }
    }

    /**
     * All messages in the batch are corrupted (payload is random garbage bytes).
     * Expected: no messages enqueued, discardCorruptedBatchMessage called,
     * incrementNumReceiveFailed invoked once.
     */
    @Test
    public void testAllMessagesCorruptedInBatch() {
        BrokerEntryMetadata brokerEntryMetadata =
                new BrokerEntryMetadata().setBrokerTimestamp(1).setIndex(1);

        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(3);

        // Construct corrupted payload: invalid metadata size triggers parse failure
        // in deSerializeSingleMessageInBatch -> readUnsignedInt returns huge value
        ByteBuf corruptedPayload = Unpooled.buffer(100);
        corruptedPayload.writeInt(Integer.MAX_VALUE);
        corruptedPayload.writeBytes(new byte[50]);

        consumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata, 0, null,
                corruptedPayload, new MessageIdData().setLedgerId(1000).setEntryId(1),
                mockCnx, DEFAULT_CONSUMER_EPOCH, false);

        // No valid messages should be enqueued
        assertEquals(consumer.numMessagesInQueue(), 0,
                "Corrupted batch should not produce any messages in queue");

        // Verify discardCorruptedBatchMessage incremented the failed counter
        verify(statsRecorder, times(1)).incrementNumReceiveFailed();
    }

    /**
     * Partial decode failure: the first message in the batch is valid,
     * but the second message is corrupted.
     * Expected: the first valid message is enqueued; remaining corrupted
     * messages are discarded.
     */
    @Test
    public void testPartialDecodeFailureInBatch() throws Exception {
        BrokerEntryMetadata brokerEntryMetadata =
                new BrokerEntryMetadata().setBrokerTimestamp(1).setIndex(1);

        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(3);

        ByteBuf batchBuffer = Unpooled.buffer(1000);

        // First message: valid, properly serialized using Commands utility
        Commands.serializeSingleMessageInBatchWithPayload(
                new SingleMessageMetadata().setPartitionKey("key1"),
                Unpooled.wrappedBuffer("hello".getBytes()), batchBuffer);

        // Second message: corrupted payload (invalid metadata size)
        batchBuffer.writeInt(Integer.MAX_VALUE);
        batchBuffer.writeBytes(new byte[20]);

        consumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata, 0, null,
                batchBuffer, new MessageIdData().setLedgerId(2000).setEntryId(2),
                mockCnx, DEFAULT_CONSUMER_EPOCH, false);

        // The first valid message should be enqueued via executeNotifyCallback,
        // which uses internalPinnedExecutor.execute(). Since it's async,
        // we check the stats synchronously — discardCorruptedBatchMessage
        // is called inline in the catch block.
        verify(statsRecorder, times(1)).incrementNumReceiveFailed();
    }

    /**
     * Empty payload with non-zero batchSize: the buffer has no readable bytes
     * but batchSize declares 2 messages.
     * Expected: IndexOutOfBoundsException during readUnsignedInt, entire batch discarded.
     */
    @Test
    public void testEmptyPayloadWithNonZeroBatchSize() {
        BrokerEntryMetadata brokerEntryMetadata =
                new BrokerEntryMetadata().setBrokerTimestamp(1).setIndex(1);

        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(2);

        // Empty buffer: readUnsignedInt() will throw IndexOutOfBoundsException
        ByteBuf emptyPayload = Unpooled.buffer(0);

        consumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata, 0, null,
                emptyPayload, new MessageIdData().setLedgerId(3000).setEntryId(3),
                mockCnx, DEFAULT_CONSUMER_EPOCH, false);

        // No messages should be enqueued
        assertEquals(consumer.numMessagesInQueue(), 0,
                "Empty payload should not produce any messages in queue");

        // The receive-failed counter should have been incremented
        verify(statsRecorder, times(1)).incrementNumReceiveFailed();
    }

    /**
     * Truncated single-message payload: metadata size is valid but payload
     * data is truncated (fewer bytes than declared payloadSize).
     * Expected: exception during retainedSlice, entire batch discarded.
     */
    @Test
    public void testTruncatedSingleMessagePayload() {
        BrokerEntryMetadata brokerEntryMetadata =
                new BrokerEntryMetadata().setBrokerTimestamp(1).setIndex(1);

        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(1);

        ByteBuf truncatedBuffer = Unpooled.buffer(100);

        // Write a valid SingleMessageMetadata with a large payloadSize,
        // but provide fewer actual bytes than declared.
        // This causes retainedSlice(readerIndex, 9999) to fail with
        // IndexOutOfBoundsException.
        SingleMessageMetadata smm = new SingleMessageMetadata()
                .setPartitionKey("key-truncated")
                .setPayloadSize(9999);  // Claims 9999 bytes of payload
        truncatedBuffer.writeInt(smm.getSerializedSize());
        smm.writeTo(truncatedBuffer);
        // Only write 5 bytes instead of 9999
        truncatedBuffer.writeBytes(new byte[5]);

        consumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata, 0, null,
                truncatedBuffer, new MessageIdData().setLedgerId(4000).setEntryId(4),
                mockCnx, DEFAULT_CONSUMER_EPOCH, false);

        // No messages should be enqueued
        assertEquals(consumer.numMessagesInQueue(), 0,
                "Truncated payload should not produce any messages in queue");

        // The receive-failed counter should have been incremented
        verify(statsRecorder, times(1)).incrementNumReceiveFailed();
    }
}
