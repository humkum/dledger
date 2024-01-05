/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.store.file;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.utils.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

public class TransientStorePool {
    private static final Logger log = LoggerFactory.getLogger(TransientStorePool.class);

    private final int poolSize;
    private final int dataFileSize;
    private final int indexFileSize;
    private final Deque<ByteBuffer> availableDataBuffers;
    private final Deque<ByteBuffer> availableIndexBuffers;
    private final DLedgerConfig dLedgerConfig;

    public TransientStorePool(final DLedgerConfig dLedgerConfig) {
        this.dLedgerConfig = dLedgerConfig;
        this.poolSize = dLedgerConfig.getTransientStorePoolSize();
        this.dataFileSize = dLedgerConfig.getMappedFileSizeForEntryData();
        this.indexFileSize = dLedgerConfig.getMappedFileSizeForEntryIndex();
        this.availableDataBuffers = new ConcurrentLinkedDeque<>();
        this.availableIndexBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     */
    public void init() {
        long startTime = System.currentTimeMillis();
        initByteBuffers(availableDataBuffers, dataFileSize);
        initByteBuffers(availableIndexBuffers, indexFileSize);
        log.info("init direct byte buffer success, cost {}ms", System.currentTimeMillis() - startTime);
    }

    public void initByteBuffers(Deque<ByteBuffer> byteBuffers, int fileSize) {
        for (int i = 0; i < poolSize; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            byteBuffers.offer(byteBuffer);
        }
    }

    public void destroy() {
        destroyDirectByteBuffer(availableDataBuffers);
        destroyDirectByteBuffer(availableIndexBuffers);
    }

    public void destroyDirectByteBuffer(Deque<ByteBuffer> byteBuffers) {
        for (ByteBuffer byteBuffer : byteBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(dataFileSize));
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        int capacity = byteBuffer.capacity();
        if (capacity == dataFileSize) {
            log.info("return a data buffer to transient store, available data buffer number: {}", availableBufferNums());
            byteBuffer.position(0);
            byteBuffer.limit(dataFileSize);
            this.availableDataBuffers.offerFirst(byteBuffer);
        } else if (capacity == indexFileSize) {
            log.info("return a index buffer to transient store, available index buffer number: {}", availableIndexBufferNums());
            byteBuffer.position(0);
            byteBuffer.limit(indexFileSize);
            this.availableIndexBuffers.offerFirst(byteBuffer);
        }
    }

    public ByteBuffer borrowBuffer(int fileSize) {
        ByteBuffer buffer = null;
        if (fileSize == dataFileSize) {
            buffer = availableDataBuffers.pollFirst();
            log.info("borrow a new buffer from transient store, available data buffer number: {}", availableBufferNums());
            if (availableDataBuffers.size() < poolSize * 0.4) {
                log.warn("TransientStorePool only remain {} data sheets.", availableDataBuffers.size());
            }
        } else if (fileSize == indexFileSize) {
            buffer = availableIndexBuffers.pollFirst();
            log.info("borrow a new buffer from transient store, available index buffer number: {}", availableIndexBufferNums());
            if (availableIndexBuffers.size() < poolSize * 0.4) {
                log.warn("TransientStorePool only remain {} index sheets.", availableIndexBuffers.size());
            }
        } else {
            log.warn("unknown error, fileSize illegal {}", fileSize);
        }
        return buffer;
    }

    public int availableBufferNums() {
        if (dLedgerConfig.isTransientStorePoolEnable()) {
            return availableDataBuffers.size();
        }
        return Integer.MAX_VALUE;
    }

    public int availableIndexBufferNums() {
        if (dLedgerConfig.isTransientStorePoolEnable()) {
            return availableIndexBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
