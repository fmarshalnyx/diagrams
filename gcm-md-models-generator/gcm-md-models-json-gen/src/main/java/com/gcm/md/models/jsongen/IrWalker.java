package com.gcm.md.models.jsongen;

import uk.co.real_logic.sbe.ir.Encoding;
import uk.co.real_logic.sbe.ir.Signal;
import uk.co.real_logic.sbe.ir.Token;

import java.util.ArrayList;
import java.util.List;

/**
 * Walks the sbe-tool IR token stream for one message and builds a {@link Model.Message}.
 *
 * IR token structure reminder:
 *   BEGIN_MESSAGE
 *     BEGIN_FIELD { ENCODING | BEGIN_ENUM..END_ENUM | BEGIN_SET..END_SET | BEGIN_COMPOSITE..END_COMPOSITE } END_FIELD
 *     BEGIN_GROUP  BEGIN_COMPOSITE(dimensions)..END_COMPOSITE  fields/groups/varData...  END_GROUP
 *     BEGIN_VAR_DATA BEGIN_COMPOSITE(length+varData)..END_COMPOSITE END_VAR_DATA
 *   END_MESSAGE
 *
 * componentTokenCount() on any BEGIN_* token spans the whole construct including its END_* token.
 */
final class IrWalker
{
    private IrWalker() {}

    static Model.Message walk(final List<Token> tokens)
    {
        final Token msgToken = tokens.get(0); // BEGIN_MESSAGE
        final List<Model.Prop> props = walkBody(tokens, 1, tokens.size() - 1);
        return new Model.Message(msgToken.name(), msgToken.id(), props);
    }

    private static List<Model.Prop> walkBody(final List<Token> tokens, final int start, final int endExclusive)
    {
        final List<Model.Prop> props = new ArrayList<>();
        int i = start;
        while (i < endExclusive)
        {
            final Token t = tokens.get(i);
            switch (t.signal())
            {
                case BEGIN_FIELD:
                    props.add(parseField(tokens, i));
                    i += t.componentTokenCount();
                    break;
                case BEGIN_GROUP:
                    props.add(parseGroup(tokens, i));
                    i += t.componentTokenCount();
                    break;
                case BEGIN_VAR_DATA:
                    props.add(parseVarData(tokens, i));
                    i += t.componentTokenCount();
                    break;
                default:
                    i++;
                    break;
            }
        }
        return props;
    }

    private static Model.Prop parseField(final List<Token> tokens, final int fieldIndex)
    {
        final Token fieldToken = tokens.get(fieldIndex);
        final Token typeToken = tokens.get(fieldIndex + 1);
        final String fieldName = fieldToken.name();

        switch (typeToken.signal())
        {
            case ENCODING:
                return primitive(fieldName, fieldToken, typeToken);
            case BEGIN_ENUM:
                return enumProp(fieldName, fieldToken, tokens, fieldIndex + 1);
            case BEGIN_SET:
                return setProp(fieldName, tokens, fieldIndex + 1);
            case BEGIN_COMPOSITE:
                return compositeProp(fieldName, tokens, fieldIndex + 1);
            default:
                throw new IllegalStateException(
                    "Unexpected token " + typeToken.signal() + " inside field " + fieldName);
        }
    }

    private static Model.PrimitiveProp primitive(final String fieldName, final Token fieldToken, final Token typeToken)
    {
        final Encoding enc = typeToken.encoding();
        final boolean optional = isOptional(fieldToken, typeToken);
        final boolean constant = enc.presence() == Encoding.Presence.CONSTANT
            || fieldToken.encoding().presence() == Encoding.Presence.CONSTANT;
        final String constValue = constant && enc.constValue() != null ? enc.constValue().toString() : null;
        final String semantic = enc.semanticType() != null
            ? enc.semanticType() : fieldToken.encoding().semanticType();

        return new Model.PrimitiveProp(
            fieldName,
            enc.primitiveType(),
            typeToken.arrayLength(),
            enc.characterEncoding(),
            semantic,
            optional,
            constant,
            constValue);
    }

    private static Model.EnumProp enumProp(final String fieldName, final Token fieldToken,
                                           final List<Token> tokens, final int enumIndex)
    {
        final Token enumToken = tokens.get(enumIndex);
        final int end = enumIndex + enumToken.componentTokenCount() - 1; // END_ENUM
        final List<String> values = new ArrayList<>();
        for (int i = enumIndex + 1; i < end; i++)
        {
            final Token t = tokens.get(i);
            if (t.signal() == Signal.VALID_VALUE)
            {
                values.add(t.name());
            }
        }
        return new Model.EnumProp(fieldName, typeName(enumToken), values, isOptional(fieldToken, enumToken));
    }

    private static Model.SetProp setProp(final String fieldName, final List<Token> tokens, final int setIndex)
    {
        final Token setToken = tokens.get(setIndex);
        final int end = setIndex + setToken.componentTokenCount() - 1; // END_SET
        final List<String> choices = new ArrayList<>();
        for (int i = setIndex + 1; i < end; i++)
        {
            final Token t = tokens.get(i);
            if (t.signal() == Signal.CHOICE)
            {
                choices.add(t.name());
            }
        }
        return new Model.SetProp(fieldName, typeName(setToken), choices);
    }

    private static Model.CompositeProp compositeProp(final String fieldName, final List<Token> tokens, final int compIndex)
    {
        final Token compToken = tokens.get(compIndex);
        final int end = compIndex + compToken.componentTokenCount() - 1; // END_COMPOSITE
        final List<Model.Prop> parts = new ArrayList<>();
        int i = compIndex + 1;
        while (i < end)
        {
            final Token t = tokens.get(i);
            switch (t.signal())
            {
                case ENCODING:
                    parts.add(primitive(t.name(), t, t));
                    i++;
                    break;
                case BEGIN_COMPOSITE:
                    parts.add(compositeProp(t.name(), tokens, i));
                    i += t.componentTokenCount();
                    break;
                case BEGIN_ENUM:
                    parts.add(enumProp(t.name(), t, tokens, i));
                    i += t.componentTokenCount();
                    break;
                case BEGIN_SET:
                    parts.add(setProp(t.name(), tokens, i));
                    i += t.componentTokenCount();
                    break;
                default:
                    i++;
                    break;
            }
        }
        return new Model.CompositeProp(fieldName, typeName(compToken), parts);
    }

    private static Model.GroupProp parseGroup(final List<Token> tokens, final int groupIndex)
    {
        final Token groupToken = tokens.get(groupIndex);
        final int end = groupIndex + groupToken.componentTokenCount() - 1; // END_GROUP

        // Skip the dimension composite (blockLength + numInGroup) that follows BEGIN_GROUP.
        int i = groupIndex + 1;
        final Token dims = tokens.get(i);
        if (dims.signal() == Signal.BEGIN_COMPOSITE)
        {
            i += dims.componentTokenCount();
        }

        return new Model.GroupProp(groupToken.name(), walkBody(tokens, i, end));
    }

    private static Model.VarDataProp parseVarData(final List<Token> tokens, final int varIndex)
    {
        final Token varToken = tokens.get(varIndex);
        final int end = varIndex + varToken.componentTokenCount() - 1; // END_VAR_DATA
        String characterEncoding = null;
        for (int i = varIndex + 1; i < end; i++)
        {
            final Token t = tokens.get(i);
            if (t.signal() == Signal.ENCODING && "varData".equals(t.name()))
            {
                characterEncoding = t.encoding().characterEncoding();
            }
        }
        return new Model.VarDataProp(varToken.name(), characterEncoding);
    }

    private static boolean isOptional(final Token fieldToken, final Token typeToken)
    {
        return fieldToken.encoding().presence() == Encoding.Presence.OPTIONAL
            || typeToken.encoding().presence() == Encoding.Presence.OPTIONAL;
    }

    /** Type name of a referenced type token (referencedName when the type was declared in &lt;types&gt;). */
    static String typeName(final Token token)
    {
        final String referenced = token.referencedName();
        return referenced != null ? referenced : token.name();
    }

    static String upperFirst(final String s)
    {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }
}
