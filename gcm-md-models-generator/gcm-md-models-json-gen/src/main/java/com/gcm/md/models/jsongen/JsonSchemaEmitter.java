package com.gcm.md.models.jsongen;

import uk.co.real_logic.sbe.PrimitiveType;

import java.util.ArrayList;
import java.util.List;

/**
 * Emits one JSON Schema (draft 2020-12) per message.
 *
 * Conventions (must match TranslatorEmitter exactly):
 *  - enums            -> string with enum of value NAMES
 *  - sets (bitfields) -> array of choice-name strings, uniqueItems
 *  - groups           -> array of objects
 *  - composites       -> nested objects
 *  - char arrays      -> string (translator trims trailing NULs), maxLength = declared length
 *  - int64            -> string of decimal digits (avoids JS 2^53 precision loss)
 *  - uint64           -> string of decimal digits (unsigned)
 *  - optional fields  -> not in "required"; translator OMITS the key at null (never emits JSON null)
 *  - timestamps (semanticType UTCTimestamp) -> string of epoch nanoseconds
 */
final class JsonSchemaEmitter
{
    private JsonSchemaEmitter() {}

    static String emit(final Model.Message m)
    {
        final StringBuilder b = new StringBuilder(2048);
        b.append("{\n");
        b.append("  \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n");
        b.append("  \"$id\": \"urn:gcm:md:models:").append(m.name).append("\",\n");
        b.append("  \"title\": \"").append(m.name).append("\",\n");
        b.append("  \"description\": \"Generated from SBE message ").append(m.name)
            .append(" (templateId ").append(m.templateId).append(")\",\n");
        objectBody(b, "  ", m.props);
        b.append("\n}\n");
        return b.toString();
    }

    private static void objectBody(final StringBuilder b, final String ind, final List<Model.Prop> props)
    {
        b.append(ind).append("\"type\": \"object\",\n");
        b.append(ind).append("\"additionalProperties\": false,\n");

        final List<String> required = new ArrayList<>();
        for (final Model.Prop p : props)
        {
            if (skipped(p) || isOptional(p))
            {
                continue;
            }
            required.add(p.name);
        }
        b.append(ind).append("\"required\": [");
        for (int i = 0; i < required.size(); i++)
        {
            if (i > 0)
            {
                b.append(", ");
            }
            b.append('"').append(required.get(i)).append('"');
        }
        b.append("],\n");

        b.append(ind).append("\"properties\": {");
        boolean first = true;
        for (final Model.Prop p : props)
        {
            if (skipped(p))
            {
                continue;
            }
            if (!first)
            {
                b.append(',');
            }
            first = false;
            b.append('\n').append(ind).append("  \"").append(p.name).append("\": ");
            propSchema(b, ind + "  ", p);
        }
        b.append('\n').append(ind).append('}');
    }

    private static void propSchema(final StringBuilder b, final String ind, final Model.Prop p)
    {
        if (p instanceof Model.PrimitiveProp)
        {
            primitiveSchema(b, (Model.PrimitiveProp)p);
        }
        else if (p instanceof Model.EnumProp)
        {
            final Model.EnumProp e = (Model.EnumProp)p;
            b.append("{ \"type\": \"string\", \"enum\": [");
            for (int i = 0; i < e.values.size(); i++)
            {
                if (i > 0)
                {
                    b.append(", ");
                }
                b.append('"').append(e.values.get(i)).append('"');
            }
            b.append("] }");
        }
        else if (p instanceof Model.SetProp)
        {
            final Model.SetProp s = (Model.SetProp)p;
            b.append("{ \"type\": \"array\", \"uniqueItems\": true, \"items\": { \"type\": \"string\", \"enum\": [");
            for (int i = 0; i < s.choices.size(); i++)
            {
                if (i > 0)
                {
                    b.append(", ");
                }
                b.append('"').append(s.choices.get(i)).append('"');
            }
            b.append("] } }");
        }
        else if (p instanceof Model.CompositeProp)
        {
            final Model.CompositeProp c = (Model.CompositeProp)p;
            b.append("{\n");
            objectBody(b, ind + "  ", c.parts);
            b.append('\n').append(ind).append('}');
        }
        else if (p instanceof Model.GroupProp)
        {
            final Model.GroupProp g = (Model.GroupProp)p;
            b.append("{\n");
            b.append(ind).append("  \"type\": \"array\",\n");
            b.append(ind).append("  \"items\": {\n");
            objectBody(b, ind + "    ", g.members);
            b.append('\n').append(ind).append("  }\n");
            b.append(ind).append('}');
        }
        else if (p instanceof Model.VarDataProp)
        {
            b.append("{ \"type\": \"string\" }");
        }
    }

    private static void primitiveSchema(final StringBuilder b, final Model.PrimitiveProp p)
    {
        if (p.constant && p.constValue != null)
        {
            if (p.type == PrimitiveType.CHAR)
            {
                b.append("{ \"const\": \"").append(p.constValue).append("\" }");
            }
            else
            {
                b.append("{ \"const\": ").append(p.constValue).append(" }");
            }
            return;
        }
        if (p.isCharArray())
        {
            b.append("{ \"type\": \"string\", \"maxLength\": ").append(p.arrayLength).append(" }");
            return;
        }
        if (p.type == PrimitiveType.CHAR)
        {
            b.append("{ \"type\": \"string\", \"minLength\": 1, \"maxLength\": 1 }");
            return;
        }
        if (p.arrayLength > 1)
        {
            b.append("{ \"type\": \"array\", \"minItems\": ").append(p.arrayLength)
                .append(", \"maxItems\": ").append(p.arrayLength)
                .append(", \"items\": { \"type\": \"").append(isFloating(p.type) ? "number" : "integer")
                .append("\" } }");
            return;
        }
        switch (p.type)
        {
            case INT64:
                b.append("{ \"type\": \"string\", \"pattern\": \"^-?[0-9]+$\", \"description\": \"")
                    .append(p.isTimestamp() ? "Epoch nanoseconds, " : "")
                    .append("int64 as decimal string\" }");
                break;
            case UINT64:
                b.append("{ \"type\": \"string\", \"pattern\": \"^[0-9]+$\", \"description\": \"")
                    .append(p.isTimestamp() ? "Epoch nanoseconds, " : "")
                    .append("uint64 as decimal string\" }");
                break;
            case FLOAT:
            case DOUBLE:
                b.append("{ \"type\": \"number\" }");
                break;
            default:
                b.append("{ \"type\": \"integer\" }");
                break;
        }
    }

    private static boolean isFloating(final PrimitiveType t)
    {
        return t == PrimitiveType.FLOAT || t == PrimitiveType.DOUBLE;
    }

    private static boolean isOptional(final Model.Prop p)
    {
        if (p instanceof Model.PrimitiveProp)
        {
            return ((Model.PrimitiveProp)p).optional;
        }
        if (p instanceof Model.EnumProp)
        {
            return ((Model.EnumProp)p).optional;
        }
        if (p instanceof Model.CompositeProp)
        {
            // Delta convention: composites whose data parts are all optional are
            // omitted entirely when null (e.g. PRICENULL9 fields in a DELTA tick).
            return ((Model.CompositeProp)p).effectivelyOptional();
        }
        return false;
    }

    /** Binary var-data is not representable in this JSON mapping and is skipped entirely. */
    private static boolean skipped(final Model.Prop p)
    {
        return p instanceof Model.VarDataProp && !((Model.VarDataProp)p).isString();
    }
}
