package gcm.md.linehandlertemplate.relay;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceSeqNumStamperTest {

    private static final int OFFSET = 64;

    @Test
    void stampsAtTheConfiguredOffsetLittleEndian() {
        SourceSeqNumStamper stamper = new SourceSeqNumStamper(OFFSET);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[OFFSET + 4]);

        stamper.stamp(buffer, 42L);

        assertThat(buffer.getInt(OFFSET, ByteOrder.LITTLE_ENDIAN)).isEqualTo(42);
    }

    @Test
    void acceptsTheFullUint32Range() {
        SourceSeqNumStamper stamper = new SourceSeqNumStamper(OFFSET);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[OFFSET + 4]);

        stamper.stamp(buffer, 0xFFFFFFFFL);

        assertThat(buffer.getInt(OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFFFFFFL).isEqualTo(0xFFFFFFFFL);
    }

    @Test
    void rejectsValuesThatDoNotFitAUint32RatherThanSilentlyWrapping() {
        SourceSeqNumStamper stamper = new SourceSeqNumStamper(OFFSET);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[OFFSET + 4]);

        assertThatThrownBy(() -> stamper.stamp(buffer, 0x100000000L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> stamper.stamp(buffer, -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
