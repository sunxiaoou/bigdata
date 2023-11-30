package xo.disruptor;

// https://lmax-exchange.github.io/disruptor/user-guide/index.html#_getting_started

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.nio.ByteBuffer;

public class LongEventMain {
    public static void handleEvent(LongEvent event, long sequence, boolean endOfBatch) {
        System.out.println(event);
    }

    public static void translate(LongEvent event, long sequence, ByteBuffer buffer) {
        event.set(buffer.getLong(0));
    }

    public static void main(String[] args) throws Exception {
        // Specify the size of the ring buffer, must be power of 2
        int bufferSize = 1024;
        // Construct the Disruptor, with a method reference as event factory
        Disruptor<LongEvent> disruptor = new Disruptor<>(LongEvent::new, bufferSize, DaemonThreadFactory.INSTANCE);
        // Connect an anonymous lambda as event handler (consumer)
//        disruptor.handleEventsWith((event, sequence, endOfBatch) -> System.out.println("Event: " + event));
        // Connect a method reference as event handler (consumer)
        disruptor.handleEventsWith(LongEventMain::handleEvent);
        // Start the Disruptor, starts all threads running
        disruptor.start();
        // Get the ring buffer from the Disruptor to be used for publishing
        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();
        ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; true; l ++) {
            bb.putLong(0, l);
            // use an anonymous lambda as event producer
//            ringBuffer.publishEvent((event, sequence, buffer) -> event.set(buffer.getLong(0)), bb);
            // use a method reference as event producer
            ringBuffer.publishEvent(LongEventMain::translate, bb);
            Thread.sleep(1000);
        }
    }
}
