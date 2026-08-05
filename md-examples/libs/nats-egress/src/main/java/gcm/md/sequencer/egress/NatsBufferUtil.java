package gcm.md.sequencer.egress;

import org.agrona.DirectBuffer;

/** Shared by {@link JetStreamDestination} and {@link NatsIngressTransport}: avoids a copy when the buffer is already a plain byte array. */
final class NatsBufferUtil {

    private NatsBufferUtil() {
    }

    static byte[] toByteArray(DirectBuffer buffer, int offset, int length) {
        byte[] backing = buffer.byteArray();
        if (backing != null && offset == 0 && length == backing.length) {
            return backing;
        }
        byte[] exact = new byte[length];
        buffer.getBytes(offset, exact, 0, length);
        return exact;
    }
}
