public final class SbeJsonBridge {
    private final Int2ObjectHashMap<JsonTranslator> translators; // by templateId
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final UnsafeBuffer buf = new UnsafeBuffer();
    private final ExpandableArrayBuffer out = new ExpandableArrayBuffer(4096);

    void onMessage(Message msg) {
        byte[] data = msg.getData();
        buf.wrap(data);
        header.wrap(buf, 0);

        JsonTranslator t = translators.get(header.templateId());
        if (t == null) { unknown(msg); return; }

        int len = t.translate(buf, MessageHeaderDecoder.ENCODED_LENGTH,
                              header.blockLength(), header.version(), out);

        String outSubject = "TICK_JSON" + msg.getSubject().substring("TICK_SBE".length());
        nats.publish(outSubject, Arrays.copyOfRange(out.byteArray(), 0, len));
    }
}
