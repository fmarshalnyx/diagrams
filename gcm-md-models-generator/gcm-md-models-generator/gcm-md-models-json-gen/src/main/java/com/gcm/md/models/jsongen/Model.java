package com.gcm.md.models.jsongen;

import uk.co.real_logic.sbe.PrimitiveType;

import java.util.List;

/**
 * Simplified, generator-friendly view of an SBE message, built by {@link IrWalker}
 * from the sbe-tool IR token stream. All emitters (JSON Schema, AsyncAPI,
 * translator source) work from this model so they cannot drift from one another.
 */
public final class Model
{
    private Model() {}

    public static final class Message
    {
        public final String name;
        public final int templateId;
        public final List<Prop> props;

        public Message(final String name, final int templateId, final List<Prop> props)
        {
            this.name = name;
            this.templateId = templateId;
            this.props = props;
        }
    }

    public abstract static class Prop
    {
        public final String name;

        protected Prop(final String name)
        {
            this.name = name;
        }
    }

    /** A field of primitive type (possibly a fixed-length array, e.g. char[12]). */
    public static final class PrimitiveProp extends Prop
    {
        public final PrimitiveType type;
        public final int arrayLength;
        public final String characterEncoding; // null if none
        public final String semanticType;      // null if none
        public final boolean optional;

        public PrimitiveProp(final String name, final PrimitiveType type, final int arrayLength,
                             final String characterEncoding, final String semanticType, final boolean optional)
        {
            super(name);
            this.type = type;
            this.arrayLength = arrayLength;
            this.characterEncoding = characterEncoding;
            this.semanticType = semanticType;
            this.optional = optional;
        }

        public boolean isCharArray()
        {
            return type == PrimitiveType.CHAR && arrayLength > 1;
        }

        public boolean isTimestamp()
        {
            return semanticType != null && semanticType.contains("UTCTimestamp");
        }
    }

    public static final class EnumProp extends Prop
    {
        public final String typeName;
        public final List<String> values;
        public final boolean optional;

        public EnumProp(final String name, final String typeName, final List<String> values, final boolean optional)
        {
            super(name);
            this.typeName = typeName;
            this.values = values;
            this.optional = optional;
        }
    }

    /** A bitset field; emitted to JSON as an array of the set choice names. */
    public static final class SetProp extends Prop
    {
        public final String typeName;
        public final List<String> choices;

        public SetProp(final String name, final String typeName, final List<String> choices)
        {
            super(name);
            this.typeName = typeName;
            this.choices = choices;
        }
    }

    /** A composite field; emitted to JSON as a nested object. */
    public static final class CompositeProp extends Prop
    {
        public final String typeName;
        public final List<Prop> parts;

        public CompositeProp(final String name, final String typeName, final List<Prop> parts)
        {
            super(name);
            this.typeName = typeName;
            this.parts = parts;
        }
    }

    /** A repeating group; emitted to JSON as an array of objects. */
    public static final class GroupProp extends Prop
    {
        public final List<Prop> members;

        public GroupProp(final String name, final List<Prop> members)
        {
            super(name);
            this.members = members;
        }
    }

    /** Var-length data field. Only string-encoded var data is emitted to JSON. */
    public static final class VarDataProp extends Prop
    {
        public final String characterEncoding; // null => binary blob (skipped)

        public VarDataProp(final String name, final String characterEncoding)
        {
            super(name);
            this.characterEncoding = characterEncoding;
        }

        public boolean isString()
        {
            return characterEncoding != null;
        }
    }
}
