package com.gcm.md.models.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gcm.md.models.json.translators.GcmMdTranslators;
import com.gcm.md.models.sbe.MessageHeaderDecoder;
import com.gcm.md.models.sbe.MessageHeaderEncoder;
import com.gcm.md.models.sbe.QuoteUpdateEncoder;
import com.gcm.md.models.sbe.Side;
import com.gcm.md.models.sbe.TradeUpdateEncoder;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Encodes example messages with the generated SBE encoders, runs them through the
 * generated JSON translators, then checks both the concrete values and that the
 * output validates against the generated JSON Schemas.
 */
class RoundTripTest
{
    private final ObjectMapper mapper = new ObjectMapper();
    private final Int2ObjectHashMap<JsonTranslator> translators = GcmMdTranslators.create();

    @Test
    void quoteUpdateRoundTrip() throws Exception
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final QuoteUpdateEncoder enc = new QuoteUpdateEncoder();

        enc.wrapAndApplyHeader(buffer, 0, headerEncoder);
        enc.symbol("EURUSD");
        enc.transactTime(1_719_849_600_000_000_000L);
        enc.bid().mantissa(101_250L).exponent((byte)-4);
        enc.ask().mantissa(101_260L).exponent((byte)-4);
        enc.lastQty(QuoteUpdateEncoder.lastQtyNullValue()); // optional -> key must be omitted
        enc.flags().clear().implied(true).snapshot(true);

        final QuoteUpdateEncoder.PriceLevelsEncoder levels = enc.priceLevelsCount(2);
        levels.next();
        levels.level((short)1);
        levels.price().mantissa(101_250L).exponent((byte)-4);
        levels.qty(1_000_000L);
        levels.next();
        levels.level((short)2);
        levels.price().mantissa(101_240L).exponent((byte)-4);
        levels.qty(250_000L);

        enc.venue("XNAS");

        final JsonNode json = translate(buffer);

        assertEquals("EURUSD", json.get("symbol").asText(), "char array should be trimmed of NUL padding");
        assertEquals("1719849600000000000", json.get("transactTime").asText(), "uint64 timestamp as decimal string");
        assertFalse(json.has("lastQty"), "null optional field must be omitted, not JSON null");
        assertEquals("101250", json.get("bid").get("mantissa").asText(), "int64 mantissa as decimal string");
        assertEquals(-4, json.get("bid").get("exponent").asInt());

        final Set<String> flags = new HashSet<>();
        json.get("flags").forEach(f -> flags.add(f.asText()));
        assertEquals(Set.of("implied", "snapshot"), flags, "set emitted as array of choice names");

        assertEquals(2, json.get("priceLevels").size());
        assertEquals("1000000", json.get("priceLevels").get(0).get("qty").asText());
        assertEquals(2, json.get("priceLevels").get(1).get("level").asInt());
        assertEquals("XNAS", json.get("venue").asText());

        validateAgainstSchema(json, "/schemas/QuoteUpdate.schema.json");
    }

    @Test
    void tradeUpdateRoundTrip() throws Exception
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final TradeUpdateEncoder enc = new TradeUpdateEncoder();

        enc.wrapAndApplyHeader(buffer, 0, headerEncoder);
        enc.symbol("EURUSD");
        enc.transactTime(1_719_849_601_000_000_000L);
        enc.price().mantissa(101_255L).exponent((byte)-4);
        enc.qty(500_000L);
        enc.aggressorSide(Side.BUY);
        enc.tradeId("T-20260702-000123");

        final JsonNode json = translate(buffer);

        assertEquals("BUY", json.get("aggressorSide").asText(), "enum emitted as value name");
        assertEquals("500000", json.get("qty").asText());
        assertEquals("T-20260702-000123", json.get("tradeId").asText());

        validateAgainstSchema(json, "/schemas/TradeUpdate.schema.json");
    }

    private JsonNode translate(final UnsafeBuffer buffer) throws Exception
    {
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        header.wrap(buffer, 0);

        final JsonTranslator translator = translators.get(header.templateId());
        assertNotNull(translator, "no translator registered for templateId " + header.templateId());

        final ExpandableArrayBuffer out = new ExpandableArrayBuffer(1024);
        final int length = translator.translate(
            buffer, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version(), out);

        final byte[] jsonBytes = new byte[length];
        out.getBytes(0, jsonBytes);
        final String jsonText = new String(jsonBytes, StandardCharsets.UTF_8);
        return mapper.readTree(jsonText);
    }

    private void validateAgainstSchema(final JsonNode json, final String schemaResource)
    {
        try (InputStream in = getClass().getResourceAsStream(schemaResource))
        {
            assertNotNull(in, "schema resource not found on classpath: " + schemaResource);
            final JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
            final JsonSchema schema = factory.getSchema(in);
            final Set<ValidationMessage> errors = schema.validate(json);
            assertTrue(errors.isEmpty(), "schema validation errors: " + errors);
        }
        catch (final RuntimeException e)
        {
            throw e;
        }
        catch (final Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
