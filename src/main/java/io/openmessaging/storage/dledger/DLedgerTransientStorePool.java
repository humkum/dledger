package io.openmessaging.storage.dledger;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import io.openmessaging.storage.dledger.utils.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

public class DLedgerTransientStorePool {

    private static Logger logger = LoggerFactory.getLogger(DLedgerTransientStorePool.class);

    private final int poolSize;
    private final int fileSize;
    private final Deque<ByteBuffer> availableBuffers;
    private final DLedgerConfig dLedgerConfig;


    public DLedgerTransientStorePool(final DLedgerConfig dLedgerConfig) {
        this.dLedgerConfig = dLedgerConfig;
        this.poolSize = dLedgerConfig.getTransientStorePoolSize();
        this.fileSize = dLedgerConfig.getMappedFileSizeForEntryData();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer);
        }
    }

    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            logger.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {
        if (dLedgerConfig.isdLedgerTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
