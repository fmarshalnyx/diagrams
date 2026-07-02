package com.gcm.md.models.jsongen;

import java.util.List;

/**
 * Emits an AsyncAPI 3.0 document describing both sides of the bridge:
 *   TICK_SBE.&lt;MessageName&gt;.{symbol}  - raw SBE frames (binary)
 *   TICK_JSON.&lt;MessageName&gt;.{symbol} - JSON frames validating against the per-message schema
 *
 * The JSON payloads reference the generated schema files by relative path, so the
 * document layout is: resources/asyncapi/asyncapi.yaml + resources/schemas/*.schema.json
 */
final class AsyncApiEmitter
{
    private AsyncApiEmitter() {}

    static String emit(final List<Model.Message> messages, final int schemaId, final int schemaVersion)
    {
        final StringBuilder y = new StringBuilder(4096);
        y.append("asyncapi: 3.0.0\n");
        y.append("info:\n");
        y.append("  title: GCM Market Data\n");
        y.append("  version: '").append(schemaVersion).append("'\n");
        y.append("  description: >-\n");
        y.append("    Market data dissemination over NATS. Generated from gcm-md-sbe.xml\n");
        y.append("    (SBE schemaId ").append(schemaId).append(", version ").append(schemaVersion).append(").\n");
        y.append("    Raw SBE frames are published on TICK_SBE.* and JSON translations on TICK_JSON.*.\n");
        y.append("defaultContentType: application/json\n");

        y.append("channels:\n");
        for (final Model.Message m : messages)
        {
            y.append("  tickSbe").append(m.name).append(":\n");
            y.append("    address: TICK_SBE.").append(m.name).append(".{symbol}\n");
            y.append("    description: SBE-encoded ").append(m.name).append(" ticks (binary)\n");
            y.append("    parameters:\n");
            y.append("      symbol:\n");
            y.append("        description: Instrument symbol\n");
            y.append("    messages:\n");
            y.append("      ").append(m.name).append("Sbe:\n");
            y.append("        name: ").append(m.name).append("\n");
            y.append("        contentType: application/octet-stream\n");
            y.append("        summary: SBE ").append(m.name)
                .append(" (templateId ").append(m.templateId)
                .append(", schemaId ").append(schemaId)
                .append(", version ").append(schemaVersion).append(")\n");

            y.append("  tickJson").append(m.name).append(":\n");
            y.append("    address: TICK_JSON.").append(m.name).append(".{symbol}\n");
            y.append("    description: JSON translation of ").append(m.name).append(" ticks\n");
            y.append("    parameters:\n");
            y.append("      symbol:\n");
            y.append("        description: Instrument symbol\n");
            y.append("    messages:\n");
            y.append("      ").append(m.name).append(":\n");
            y.append("        name: ").append(m.name).append("\n");
            y.append("        contentType: application/json\n");
            y.append("        payload:\n");
            y.append("          $ref: '../schemas/").append(m.name).append(".schema.json'\n");
        }

        y.append("operations:\n");
        for (final Model.Message m : messages)
        {
            y.append("  receiveTickJson").append(m.name).append(":\n");
            y.append("    action: receive\n");
            y.append("    summary: Subscribe to JSON ").append(m.name).append(" ticks\n");
            y.append("    channel:\n");
            y.append("      $ref: '#/channels/tickJson").append(m.name).append("'\n");

            y.append("  receiveTickSbe").append(m.name).append(":\n");
            y.append("    action: receive\n");
            y.append("    summary: Subscribe to raw SBE ").append(m.name).append(" ticks\n");
            y.append("    channel:\n");
            y.append("      $ref: '#/channels/tickSbe").append(m.name).append("'\n");
        }

        return y.toString();
    }
}
