package com.gcm.md.models.json;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Translates one SBE-encoded message body into UTF-8 JSON bytes.
 *
 * Implementations are generated per message by gcm-md-models-json-gen and are
 * NOT thread-safe: they reuse an internal decoder flyweight and writer. Create
 * one set of translators per thread (see GcmMdTranslators.create()).
 */
public interface JsonTranslator
{
    /** SBE templateId this translator handles. */
    int templateId();

    /** SBE message name, e.g. "QuoteUpdate"; matches the subject's message-name segment. */
    String messageName();

    /**
     * Decode the SBE message body starting at {@code offset} (i.e. just past the
     * message header) and write the JSON representation into {@code out} starting at 0.
     *
     * @param buffer            buffer containing the SBE frame
     * @param offset            offset of the message body (after the SBE message header)
     * @param actingBlockLength blockLength from the message header
     * @param actingVersion     schema version from the message header
     * @param out               destination buffer for JSON bytes
     * @return number of JSON bytes written
     */
    int translate(
        DirectBuffer buffer,
        int offset,
        int actingBlockLength,
        int actingVersion,
        MutableDirectBuffer out);
}
