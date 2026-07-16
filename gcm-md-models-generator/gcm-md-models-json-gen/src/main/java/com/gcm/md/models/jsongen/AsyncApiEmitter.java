package com.gcm.md.models.jsongen;

import java.util.List;

/**
 * Emits an AsyncAPI 3.0 document describing both sides of the bridge:
 *
 *   tick.sbe.{source}.{assetClass}.{symbol}.{msgType}  - raw SBE frames (binary)
 *   tick.json.{source}.{assetClass}.{symbol}.{msgType} - JSON frames validating
 *                                                        against the per-message schema
 *
 * msgType tokens are producer-defined per message, e.g.:
 *   MarketDataTick -> delta | snap        TradeSummary  -> trade
 *   BookUpdate     -> book                OrderUpdate   -> order
 *   TradeFill      -> fill                InstrumentDefinition -> instrdef
 *   Heartbeat      -> tick.*.CME.heartbeat (no assetClass/symbol)
 *   MatchEventBoundary -> tick.*.<source>.<assetClass>.event (no symbol)
 *
 * The JSON payloads reference the generated schema files by relative path, so the
 * document layout is: resources/asyncapi/asyncapi.yaml + resources/schemas/*.schema.json
 */
final class AsyncApiEmitter
{
    private AsyncApiEmitter() {}

    static String emit(final List<Model.Message> messages, final int schemaId, final int schemaVersion,
                       final String sbePrefix, final String jsonPrefix)
    {
        final StringBuilder y = new StringBuilder(4096);
        y.append("asyncapi: 3.0.0\n");
        y.append("info:\n");
        y.append("  title: GCM Market Data\n");
        y.append("  version: '").append(schemaVersion).append("'\n");
        y.append("  description: >-\n");
        y.append("    GCM-MD normalized market data over NATS. Generated from gcm-md-sbe.xml\n");
        y.append("    (SBE schemaId ").append(schemaId).append(", version ").append(schemaVersion).append(").\n");
        y.append("    Raw SBE frames are published under ").append(sbePrefix).append(".* and JSON\n");
        y.append("    translations under ").append(jsonPrefix).append(".* with identical subject suffixes.\n");
        y.append("defaultContentType: application/json\n");

        y.append("channels:\n");
        for (final Model.Message m : messages)
        {
            y.append("  sbe").append(m.name).append(":\n");
            y.append("    address: ").append(sbePrefix).append(".{source}.{assetClass}.{symbol}.{msgType}\n");
            y.append("    description: SBE-encoded ").append(m.name).append(" (binary). Decode with templateId ")
                .append(m.templateId).append(" from the message header.\n");
            appendSubjectParameters(y);
            y.append("    messages:\n");
            y.append("      ").append(m.name).append("Sbe:\n");
            y.append("        name: ").append(m.name).append("\n");
            y.append("        contentType: application/octet-stream\n");
            y.append("        summary: SBE ").append(m.name)
                .append(" (templateId ").append(m.templateId)
                .append(", schemaId ").append(schemaId)
                .append(", version ").append(schemaVersion).append(")\n");

            y.append("  json").append(m.name).append(":\n");
            y.append("    address: ").append(jsonPrefix).append(".{source}.{assetClass}.{symbol}.{msgType}\n");
            y.append("    description: JSON translation of ").append(m.name).append(" frames\n");
            appendSubjectParameters(y);
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
            y.append("  receiveJson").append(m.name).append(":\n");
            y.append("    action: receive\n");
            y.append("    summary: Subscribe to JSON ").append(m.name).append(" frames\n");
            y.append("    channel:\n");
            y.append("      $ref: '#/channels/json").append(m.name).append("'\n");

            y.append("  receiveSbe").append(m.name).append(":\n");
            y.append("    action: receive\n");
            y.append("    summary: Subscribe to raw SBE ").append(m.name).append(" frames\n");
            y.append("    channel:\n");
            y.append("      $ref: '#/channels/sbe").append(m.name).append("'\n");
        }

        return y.toString();
    }

    private static void appendSubjectParameters(final StringBuilder y)
    {
        y.append("    parameters:\n");
        y.append("      source:\n");
        y.append("        description: Data source (e.g. CME, ICE, BBG)\n");
        y.append("      assetClass:\n");
        y.append("        description: Asset class routing token (e.g. energy)\n");
        y.append("      symbol:\n");
        y.append("        description: GCM-MD normalized symbol (e.g. NGQ2026)\n");
        y.append("      msgType:\n");
        y.append("        description: Message type token (delta, snap, trade, book, order, fill, instrdef, heartbeat, event)\n");
    }
}
