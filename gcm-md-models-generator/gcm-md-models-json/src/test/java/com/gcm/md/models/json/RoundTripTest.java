package com.gcm.md.models.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gcm.md.models.json.translators.GcmMdTranslators;
import com.usbank.gcm.md.sbe.AssetClass;
import com.usbank.gcm.md.sbe.BookUpdateEncoder;
import com.usbank.gcm.md.sbe.InstrumentType;
import com.usbank.gcm.md.sbe.MarketDataTickEncoder;
import com.usbank.gcm.md.sbe.MaturityMonthYearEncoder;
import com.usbank.gcm.md.sbe.MessageHeaderDecoder;
import com.usbank.gcm.md.sbe.MessageHeaderEncoder;
import com.usbank.gcm.md.sbe.PRICENULL9Encoder;
import com.usbank.gcm.md.sbe.TradeSummaryEncoder;
import com.usbank.gcm.md.sbe.TradingStatus;
import com.usbank.gcm.md.sbe.UpdateType;
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
 * Encodes real gcm-md messages with the generated SBE encoders, runs them through the
 * generated JSON translators, then checks both the concrete values (delta null-omission,
 * int64-as-string, enum names, set-as-array, constant exponents) and that the output
 * validates against the generated JSON Schemas.
 */
class RoundTripTest
{
    private final ObjectMapper mapper = new ObjectMapper();
    private final Int2ObjectHashMap<JsonTranslator> translators = GcmMdTranslators.create();

    @Test
    void marketDataTickDeltaRoundTrip() throws Exception
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        final MarketDataTickEncoder enc = new MarketDataTickEncoder();
        enc.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());

        // Section 1: header fields
        enc.sequenceId(1001L);
        enc.sourceTimestamp(1_719_849_600_000_000_000L);
        enc.ingestTimestamp(1_719_849_600_000_050_000L);
        enc.sequenceTimestamp(1_719_849_600_000_075_000L);
        enc.updateType(UpdateType.DELTA);
        enc.assetClass(AssetClass.ENERGY);
        enc.instrumentType(InstrumentType.FUTURE);
        enc.fieldPresence().clear().bidPrice(true).bidSize(true);
        enc.sourceSeqNum(42L);

        // Section 2: identity
        enc.symbol("NGQ2026");
        enc.sourceSymbol("NGQ6");
        enc.source("CME");
        enc.exchange("NYMX");
        enc.sourceSecurityId(812_345);
        enc.asset("NG");
        enc.securityGroup("NG");
        enc.currency("USD");
        enc.maturityMonthYear()
            .year(2026)
            .month((short)8)
            .day(MaturityMonthYearEncoder.dayNullValue())
            .week(MaturityMonthYearEncoder.weekNullValue());

        // Section 3: delta - only bidPrice/bidSize populated, askPrice explicitly null
        enc.bidPrice().mantissa(2_860_000_000L);            // $2.86/MMBtu at 10^-9
        enc.askPrice().mantissa(PRICENULL9Encoder.mantissaNullValue());
        enc.bidSize(123L);

        // Optional enum explicitly null (delta: status unchanged)
        enc.tradingStatus(TradingStatus.NULL_VAL);

        final JsonNode json = translate(buffer);

        assertEquals("1001", json.get("sequenceId").asText(), "uint64 as decimal string");
        assertEquals("1719849600000000000", json.get("sourceTimestamp").asText(), "epoch-nanos as string");
        assertEquals("DELTA", json.get("updateType").asText(), "enum as value name");
        assertEquals("ENERGY", json.get("assetClass").asText());
        assertEquals("NGQ2026", json.get("symbol").asText(), "char array trimmed of NUL padding");
        assertEquals("NGQ6", json.get("sourceSymbol").asText());
        assertEquals("CME", json.get("source").asText());

        // Populated PRICENULL9: mantissa string + constant exponent
        assertEquals("2860000000", json.get("bidPrice").get("mantissa").asText());
        assertEquals(-9, json.get("bidPrice").get("exponent").asInt(), "constant exponent");

        // Delta null-omission: whole composite dropped, not {} and not JSON null
        assertFalse(json.has("askPrice"), "null PRICENULL9 must omit the whole key");
        assertFalse(json.has("tradingStatus"), "null optional enum must be omitted");

        // MaturityMonthYear: present parts only
        final JsonNode maturity = json.get("maturityMonthYear");
        assertEquals(2026, maturity.get("year").asInt());
        assertEquals(8, maturity.get("month").asInt());
        assertFalse(maturity.has("day"), "null optional composite part must be omitted");
        assertFalse(maturity.has("week"));

        // FieldPresence bitset as array of choice names
        final Set<String> presence = new HashSet<>();
        json.get("fieldPresence").forEach(f -> presence.add(f.asText()));
        assertEquals(Set.of("bidPrice", "bidSize"), presence);

        assertEquals("123", json.get("bidSize").asText(), "int64 qty as decimal string");

        validateAgainstSchema(json, "/schemas/MarketDataTick.schema.json");
    }

    @Test
    void tradeSummaryRoundTrip() throws Exception
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);
        final TradeSummaryEncoder enc = new TradeSummaryEncoder();
        enc.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());

        enc.sequenceId(1002L);
        enc.sourceTimestamp(1_719_849_601_000_000_000L);
        enc.ingestTimestamp(1_719_849_601_000_040_000L);
        enc.symbol("NGQ2026");
        enc.sourceSymbol("NGQ6");
        enc.source("CME");
        enc.exchange("NYMX");
        enc.sourceSecurityId(812_345);
        enc.assetClass(AssetClass.ENERGY);
        enc.tradePrice().mantissa(2_861_000_000L);
        enc.tradeQty(25L);
        enc.numberOfOrders(3);
        enc.aggressorSide((short)1);
        enc.sourceTradeId(TradeSummaryEncoder.sourceTradeIdNullValue());
        enc.sourceSeqNum(43L);

        final JsonNode json = translate(buffer);

        assertEquals("2861000000", json.get("tradePrice").get("mantissa").asText());
        assertEquals(-9, json.get("tradePrice").get("exponent").asInt());
        assertEquals("25", json.get("tradeQty").asText());
        assertEquals(3, json.get("numberOfOrders").asInt());
        assertEquals(1, json.get("aggressorSide").asInt(), "plain uint8 stays a JSON number");
        assertFalse(json.has("sourceTradeId"), "null optional uint64 must be omitted");

        validateAgainstSchema(json, "/schemas/TradeSummary.schema.json");
    }

    @Test
    void bookUpdateGroupRoundTrip() throws Exception
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        final BookUpdateEncoder enc = new BookUpdateEncoder();
        enc.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());

        enc.sequenceId(1003L);
        enc.sourceTimestamp(1_719_849_602_000_000_000L);
        enc.ingestTimestamp(1_719_849_602_000_030_000L);
        enc.symbol("NGQ2026");
        enc.sourceSymbol("NGQ6");
        enc.source("CME");
        enc.sourceSecurityId(812_345);
        enc.assetClass(AssetClass.ENERGY);
        enc.updateType(UpdateType.DELTA);
        enc.sourceSeqNum(44L);

        final BookUpdateEncoder.BookEntriesEncoder entries = enc.bookEntriesCount(2);
        entries.next();
        entries.price().mantissa(2_860_000_000L);
        entries.size(100L);
        entries.numberOfOrders(5);
        entries.priceLevel((short)1);
        entries.side((short)0);
        entries.action((short)1);
        entries.tradeableSize(90);

        entries.next();
        entries.price().mantissa(PRICENULL9Encoder.mantissaNullValue()); // deleted level: no price
        entries.size(BookUpdateEncoder.BookEntriesEncoder.sizeNullValue());
        entries.numberOfOrders(BookUpdateEncoder.BookEntriesEncoder.numberOfOrdersNullValue());
        entries.priceLevel((short)2);
        entries.side((short)0);
        entries.action((short)2);
        entries.tradeableSize(BookUpdateEncoder.BookEntriesEncoder.tradeableSizeNullValue());

        final JsonNode json = translate(buffer);

        final JsonNode bookEntries = json.get("bookEntries");
        assertEquals(2, bookEntries.size(), "repeating group as array of objects");

        final JsonNode level1 = bookEntries.get(0);
        assertEquals("2860000000", level1.get("price").get("mantissa").asText());
        assertEquals("100", level1.get("size").asText());
        assertEquals(1, level1.get("priceLevel").asInt());

        final JsonNode level2 = bookEntries.get(1);
        assertFalse(level2.has("price"), "null price omitted inside group entry");
        assertFalse(level2.has("size"));
        assertFalse(level2.has("tradeableSize"));
        assertEquals(2, level2.get("action").asInt(), "delete action");

        validateAgainstSchema(json, "/schemas/BookUpdate.schema.json");
    }

    private JsonNode translate(final UnsafeBuffer buffer) throws Exception
    {
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        header.wrap(buffer, 0);

        final JsonTranslator translator = translators.get(header.templateId());
        assertNotNull(translator, "no translator registered for templateId " + header.templateId());

        final ExpandableArrayBuffer out = new ExpandableArrayBuffer(2048);
        final int length = translator.translate(
            buffer, MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version(), out);

        final byte[] jsonBytes = new byte[length];
        out.getBytes(0, jsonBytes);
        return mapper.readTree(new String(jsonBytes, StandardCharsets.UTF_8));
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
