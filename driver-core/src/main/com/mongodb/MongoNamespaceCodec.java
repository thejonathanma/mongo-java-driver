package com.mongodb;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class MongoNamespaceCodec implements Codec<MongoNamespace> {

    @Override
    public MongoNamespace decode(final BsonReader reader, final DecoderContext decoderContext) {
        return new MongoNamespace(reader.readString());
    }

    @Override
    public void encode(final BsonWriter writer, final MongoNamespace namespace, final EncoderContext encoderContext) {
        writer.writeString(namespace.getFullName());
    }

    @Override
    public Class<MongoNamespace> getEncoderClass() {
        return MongoNamespace.class;
    }
}
