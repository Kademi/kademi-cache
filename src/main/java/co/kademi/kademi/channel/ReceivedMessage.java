package co.kademi.kademi.channel;

import java.util.UUID;

/**
 *
 * @author brad
 */
public class ReceivedMessage {
        public final UUID dest;
        public final UUID source;
        public final byte[] data;

        public ReceivedMessage( UUID dest, UUID source, byte[] data ) {
            this.dest = dest;
            this.source = source;
            this.data = data;
        }

        public byte[] getData() {
            return data;
        }

        public UUID getDest() {
            return dest;
        }

        public UUID getSource() {
            return source;
        }
}
