# Prompt for coding assistant

Build a Java project (Gradle, Java 17+) that generates JSON Schemas, an AsyncAPI document, and zero-allocation SBE→JSON translator classes from an SBE schema, plus a NATS bridge service that uses them. Details below.

Naming: all generated libraries/modules use the `gcm-md-models` base name. Module layout:
- `gcm-md-models-sbe` — the standard sbe-tool generated Java codecs (encoders/decoders).
- `gcm-md-models-json-gen` — the code generator (Module 1 below); its outputs (schemas, AsyncAPI doc, translators) are compiled into an artifact named `gcm-md-models-json`.
- `gcm-md-models-json-bridge` — the bridge service (Module 2 below).

Use a consistent base package such as `com.gcm.md.models` (`.sbe`, `.json`, `.bridge` sub-packages).

## Inputs and dependencies

- SBE schema: `src/main/resources/gcm-md-sbe.xml` (real-logic Simple Binary Encoding XML).
- Dependencies: `uk.co.real-logic:sbe-tool`, `org.agrona:agrona`, `io.nats:jnats`, and for tests `com.networknt:json-schema-validator` and JUnit 5.
- Use sbe-tool's existing classes: parse the XML with `XmlSchemaParser`, produce IR with `IrGenerator`, and generate the standard Java codecs (encoders/decoders/flyweights) as part of the build.

## Module 1: code generator (`gcm-md-models-json-gen`)

A generator class (runnable as a Gradle task) that walks the SBE IR (`uk.co.real_logic.sbe.ir.Ir`) token stream and, for every message, emits three artifacts into `build/generated`:

### 1a. JSON Schema (draft 2020-12), one file per message

Mapping conventions (apply these consistently everywhere):
- SBE enums → `"type": "string"` with `enum` listing the enum **value names** (not raw codes).
- SBE sets (bitfields) → array of strings, items enum = choice names.
- Repeating groups → `"type": "array"` with `items` referencing a `$defs` sub-schema for the group element.
- Composites → nested objects.
- Fixed-length char arrays → string (translator trims trailing nulls/padding); include `maxLength`.
- Var-length data (varStringEncoding) → string.
- int8/16/32, uint8/16 → `integer`. **int64/uint64 → `"type": "string"`** (to avoid JS 2^53 precision loss).
- float/double → `number`.
- Fields whose type declares a `nullValue` are optional: not listed in `required`, and the translator **omits the key** when the value equals the null sentinel (never emit JSON `null`).
- Timestamp fields (semanticType `UTCTimestamp` or similar) → string containing epoch nanoseconds.
- Field name casing: use the SBE field names as-is (camelCase).

### 1b. AsyncAPI 3.0 document (single YAML file)

- Channels `TICK_SBE.{message}.{symbol}` and `TICK_JSON.{message}.{symbol}` per message, with `{symbol}` as a channel parameter, NATS bindings.
- TICK_SBE messages: contentType `application/octet-stream`, description referencing the SBE templateId, schemaId, and version (documentation only).
- TICK_JSON messages: contentType `application/json`, payload `$ref` to the schemas from 1a (embed under `components/schemas`).
- Both generated from the same IR walk so names/shapes cannot drift.

### 1c. One `JsonTranslator` implementation per message

Interface:

```java
public interface JsonTranslator {
    /** Writes UTF-8 JSON for one message body into out at offset 0; returns byte length. */
    int translate(DirectBuffer buffer, int offset, int actingBlockLength,
                  int actingVersion, MutableDirectBuffer out);
    int templateId();
}
```

Requirements:
- Each translator wraps the message's generated SBE flyweight decoder (held as a field, rewrapped per call — instances are single-threaded, document this).
- Write JSON bytes directly into the `MutableDirectBuffer` (hand-rolled append of literals/numbers/escaped strings, or a small shared `JsonWriter` helper class you also generate/handwrite). No Jackson, no POJOs, no per-call allocation on the hot path (string escaping may allocate only when escaping is actually required).
- Honor exactly the conventions from 1a (enum names, omitted nulls, int64-as-string, trimmed char arrays, groups as arrays, epoch-nanos strings).
- Also generate a `TranslatorRegistry` class exposing `Int2ObjectHashMap<JsonTranslator> byTemplateId()`.

IR-walking notes: the token stream uses BEGIN_MESSAGE/END_MESSAGE, BEGIN_FIELD/END_FIELD, BEGIN_GROUP/END_GROUP, BEGIN_COMPOSITE/END_COMPOSITE, BEGIN_ENUM, BEGIN_SET, and VAR_DATA tokens; recurse on begin/end pairs and use `token.encoding()` for primitive type, nullValue, and semanticType.

## Module 2: bridge service (`gcm-md-models-json-bridge`)

A small service using jnats:
- Subscribes to `TICK_SBE.>` on a single dispatcher; all decoder/translator/buffer state is confined to that thread (fields: `MessageHeaderDecoder`, reusable `UnsafeBuffer`, `ExpandableArrayBuffer` for output, the registry map).
- Per message: wrap bytes, decode the SBE message header, look up translator by `templateId()` — trust the header over the subject; if the subject's message-name segment disagrees with the templateId, log a warning and drop. Unknown templateId: count + log at most once per id, drop.
- Output subject = input subject with prefix `TICK_SBE` replaced by `TICK_JSON`.
- Publish the JSON bytes (single `byte[]` copy from the output buffer — the only allocation per message).
- Config via env vars: NATS URL, subject prefixes. Graceful shutdown, drain on SIGTERM.

## Build wiring

Gradle tasks in order: (1) run sbe-tool to generate Java codecs from `gcm-md-sbe.xml` into `gcm-md-models-sbe`, (2) run the `gcm-md-models-json-gen` generator (IR walk) producing schemas + AsyncAPI + translators, (3) compile everything together. Generated sources under `build/generated` registered as source sets. `./gradlew build` does all of it.

## Tests

- Round-trip per message: encode a populated sample with the generated SBE encoder → translate to JSON → parse and validate against the generated JSON Schema (networknt validator) → assert field values, including: an enum rendered by name, an omitted optional field at its nullValue, an int64 emitted as string, a repeating group with 0 and 2 elements, a var-length string, a char array with padding trimmed.
- Bridge unit test with an embedded/mocked NATS connection verifying subject rewrite and unknown-templateId drop behavior.

Start by generating for whatever messages exist in `gcm-md-sbe.xml`; if the file is absent, create a small example schema (one quote message with enum, optional int64 price, composite, repeating group, varString) so the pipeline is demonstrable end-to-end.
