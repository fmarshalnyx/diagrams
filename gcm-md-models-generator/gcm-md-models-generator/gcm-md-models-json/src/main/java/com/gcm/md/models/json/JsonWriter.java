package com.gcm.md.models.json;

import org.agrona.MutableDirectBuffer;

/**
 * Minimal, allocation-free JSON writer over an agrona MutableDirectBuffer.
 *
 * Comma placement is tracked per nesting level; name() marks that the next value
 * belongs to the name just written so it does not get its own separator.
 *
 * Numeric conventions (matching the generated JSON Schemas):
 *  - valueLong          : plain JSON number
 *  - valueLongAsString  : signed decimal string (int64)
 *  - valueULongAsString : unsigned decimal string (uint64)
 *  - valueDouble        : plain JSON number; NaN/Infinity are emitted as null
 *
 * Not thread-safe. Reuse by calling wrap() per message.
 */
public final class JsonWriter
{
    private static final int MAX_DEPTH = 64;

    private MutableDirectBuffer buf;
    private int pos;
    private int depth;
    private final boolean[] hasItems = new boolean[MAX_DEPTH];
    private boolean afterName;

    public void wrap(final MutableDirectBuffer buffer)
    {
        this.buf = buffer;
        pos = 0;
        depth = 0;
        hasItems[0] = false;
        afterName = false;
    }

    public int length()
    {
        return pos;
    }

    private void sep()
    {
        if (afterName)
        {
            afterName = false;
            return;
        }
        if (hasItems[depth])
        {
            buf.putByte(pos++, (byte)',');
        }
        hasItems[depth] = true;
    }

    public void beginObject()
    {
        sep();
        buf.putByte(pos++, (byte)'{');
        hasItems[++depth] = false;
    }

    public void endObject()
    {
        depth--;
        buf.putByte(pos++, (byte)'}');
    }

    public void beginArray()
    {
        sep();
        buf.putByte(pos++, (byte)'[');
        hasItems[++depth] = false;
    }

    public void endArray()
    {
        depth--;
        buf.putByte(pos++, (byte)']');
    }

    /** Writes "name": - the name must be a plain ASCII identifier (SBE field names are). */
    public void name(final String n)
    {
        sep();
        buf.putByte(pos++, (byte)'"');
        pos += buf.putStringWithoutLengthAscii(pos, n);
        buf.putByte(pos++, (byte)'"');
        buf.putByte(pos++, (byte)':');
        afterName = true;
    }

    public void valueString(final String s)
    {
        sep();
        buf.putByte(pos++, (byte)'"');
        writeEscaped(s, 0, s.length());
        buf.putByte(pos++, (byte)'"');
    }

    /** String from a fixed-length SBE char array: trailing NUL padding is trimmed. */
    public void valueFixedString(final String s)
    {
        int end = s.length();
        while (end > 0 && s.charAt(end - 1) == '\u0000')
        {
            end--;
        }
        sep();
        buf.putByte(pos++, (byte)'"');
        writeEscaped(s, 0, end);
        buf.putByte(pos++, (byte)'"');
    }

    public void valueLong(final long v)
    {
        sep();
        pos += buf.putLongAscii(pos, v);
    }

    /** int64 as a quoted decimal string (avoids JS 2^53 precision loss). */
    public void valueLongAsString(final long v)
    {
        sep();
        buf.putByte(pos++, (byte)'"');
        pos += buf.putLongAscii(pos, v);
        buf.putByte(pos++, (byte)'"');
    }

    /** uint64 as a quoted unsigned decimal string. */
    public void valueULongAsString(final long v)
    {
        sep();
        buf.putByte(pos++, (byte)'"');
        if (v >= 0)
        {
            pos += buf.putLongAscii(pos, v);
        }
        else
        {
            pos += buf.putStringWithoutLengthAscii(pos, Long.toUnsignedString(v));
        }
        buf.putByte(pos++, (byte)'"');
    }

    /** NaN and infinities have no JSON representation and are written as null. */
    public void valueDouble(final double v)
    {
        sep();
        if (Double.isNaN(v) || Double.isInfinite(v))
        {
            pos += buf.putStringWithoutLengthAscii(pos, "null");
        }
        else
        {
            pos += buf.putStringWithoutLengthAscii(pos, Double.toString(v));
        }
    }

    /** Single SBE char emitted as a one-character JSON string. */
    public void valueCharAscii(final byte c)
    {
        sep();
        buf.putByte(pos++, (byte)'"');
        final char ch = (char)(c & 0xFF);
        if (ch == '"' || ch == '\\' || ch < 0x20)
        {
            writeEscapedChar(ch);
        }
        else
        {
            buf.putByte(pos++, c);
        }
        buf.putByte(pos++, (byte)'"');
    }

    public void valueBoolean(final boolean v)
    {
        sep();
        pos += buf.putStringWithoutLengthAscii(pos, v ? "true" : "false");
    }

    public void valueNull()
    {
        sep();
        pos += buf.putStringWithoutLengthAscii(pos, "null");
    }

    private void writeEscaped(final String s, final int from, final int to)
    {
        for (int i = from; i < to; i++)
        {
            final char c = s.charAt(i);
            if (c == '"' || c == '\\' || c < 0x20)
            {
                writeEscapedChar(c);
            }
            else if (c < 0x80)
            {
                buf.putByte(pos++, (byte)c);
            }
            else if (Character.isHighSurrogate(c) && i + 1 < to && Character.isLowSurrogate(s.charAt(i + 1)))
            {
                final int cp = Character.toCodePoint(c, s.charAt(i + 1));
                i++;
                buf.putByte(pos++, (byte)(0xF0 | (cp >> 18)));
                buf.putByte(pos++, (byte)(0x80 | ((cp >> 12) & 0x3F)));
                buf.putByte(pos++, (byte)(0x80 | ((cp >> 6) & 0x3F)));
                buf.putByte(pos++, (byte)(0x80 | (cp & 0x3F)));
            }
            else if (c < 0x800)
            {
                buf.putByte(pos++, (byte)(0xC0 | (c >> 6)));
                buf.putByte(pos++, (byte)(0x80 | (c & 0x3F)));
            }
            else
            {
                // includes unpaired surrogates, encoded as-is per lenient UTF-8
                buf.putByte(pos++, (byte)(0xE0 | (c >> 12)));
                buf.putByte(pos++, (byte)(0x80 | ((c >> 6) & 0x3F)));
                buf.putByte(pos++, (byte)(0x80 | (c & 0x3F)));
            }
        }
    }

    private void writeEscapedChar(final char c)
    {
        buf.putByte(pos++, (byte)'\\');
        switch (c)
        {
            case '"':
                buf.putByte(pos++, (byte)'"');
                break;
            case '\\':
                buf.putByte(pos++, (byte)'\\');
                break;
            case '\b':
                buf.putByte(pos++, (byte)'b');
                break;
            case '\f':
                buf.putByte(pos++, (byte)'f');
                break;
            case '\n':
                buf.putByte(pos++, (byte)'n');
                break;
            case '\r':
                buf.putByte(pos++, (byte)'r');
                break;
            case '\t':
                buf.putByte(pos++, (byte)'t');
                break;
            default:
                buf.putByte(pos++, (byte)'u');
                pos += buf.putStringWithoutLengthAscii(pos, String.format("%04x", (int)c));
                break;
        }
    }
}
