/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model.changestream;

import com.mongodb.MongoNamespace;
import com.mongodb.assertions.Assertions;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;

/**
 * Represents the {@code $changeStream} aggregation output document.
 *
 * <p>Note: this class will not be applicable for all change stream outputs. If using custom pipelines that radically change the
 * change stream result, then an alternative document format should be used.</p>
 *
 * @param <TDocument> The type that this collection will encode the {@code fullDocument} field into.
 * @since 3.6
 */
public final class ChangeStreamDocument<TDocument> {

    @BsonId()
    private final BsonDocument resumeToken;
    private final BsonDocument namespaceDocument;
    private final TDocument fullDocument;
    private final BsonDocument documentKey;
    private final BsonTimestamp clusterTime;
    private final OperationType operationType;
    private final UpdateDescription updateDescription;

    /**
     * Creates a new instance
     *
     * @param resumeToken the resume token
     * @param namespace the namespace
     * @param documentKey a document containing the _id of the changed document
     * @param fullDocument the fullDocument
     * @param operationType the operation type
     * @param updateDescription the update description
     * @deprecated Prefer {@link #ChangeStreamDocument(BsonDocument, MongoNamespace, Object, BsonDocument, BsonTimestamp, OperationType,
     *                                                 UpdateDescription)}
     */
    @Deprecated
    public ChangeStreamDocument(final BsonDocument resumeToken,
                                final MongoNamespace namespace,
                                final TDocument fullDocument,
                                final BsonDocument documentKey,
                                final OperationType operationType,
                                final UpdateDescription updateDescription) {
        this(resumeToken, namespace, fullDocument, documentKey, null, operationType, updateDescription);
    }

    /**
     * Creates a new instance
     *
     * @param resumeToken the resume token
     * @param namespace the namespace
     * @param documentKey a document containing the _id of the changed document
     * @param clusterTime the cluster time at which the change occurred
     * @param fullDocument the fullDocument
     * @param operationType the operation type
     * @param updateDescription the update description
     */
    @Deprecated
    public ChangeStreamDocument(final BsonDocument resumeToken,
                                final MongoNamespace namespace,
                                final TDocument fullDocument,
                                final BsonDocument documentKey,
                                @Nullable final BsonTimestamp clusterTime,
                                final OperationType operationType,
                                final UpdateDescription updateDescription) {
        this(resumeToken, namespaceToDocument(namespace), fullDocument, documentKey,
                clusterTime, operationType, updateDescription);
    }

    /**
     * Creates a new instance
     *
     * @param resumeToken the resume token
     * @param namespaceDocument the BsonDocument representing the namespace
     * @param fullDocument the full document
     * @param documentKey a document containing the _id of the changed document
     * @param clusterTime the cluster time at which the change occured
     * @param operationType the operation type
     * @param updateDescription the update description
     *
     * @since 3.8
     */
    @BsonCreator
    public ChangeStreamDocument(@BsonProperty("resumeToken") final BsonDocument resumeToken,
                                @BsonProperty("ns") final BsonDocument namespaceDocument,
                                @BsonProperty("fullDocument") final TDocument fullDocument,
                                @BsonProperty("documentKey") final BsonDocument documentKey,
                                @Nullable @BsonProperty("clusterTime") final BsonTimestamp clusterTime,
                                @BsonProperty("operationType") final OperationType operationType,
                                @BsonProperty("updateDescription") final UpdateDescription updateDescription) {
        this.resumeToken = resumeToken;
        this.namespaceDocument = namespaceDocument;
        this.documentKey = documentKey;
        this.fullDocument = fullDocument;
        this.clusterTime = clusterTime;
        this.operationType = operationType;
        this.updateDescription = updateDescription;
    }

    private static BsonDocument namespaceToDocument(final MongoNamespace namespace) {
        Assertions.notNull("namespace", namespace);
        return new BsonDocument("db", new BsonString(namespace.getDatabaseName()))
                .append("coll", new BsonString(namespace.getCollectionName()));
    }

    /**
     * Returns the resumeToken
     *
     * @return the resumeToken
     */
    public BsonDocument getResumeToken() {
        return resumeToken;
    }

    /**
     * Returns the namespace
     *
     * The invalidate operation type does include a MongoNamespace in the ChangeStreamDocument response. The
     * dropDatabase operation type includes a MongoNamespace, but does not include a collection name as part
     * of the namespace.
     *
     * @return the namespace. If the namespaceDocument is null or if it is missing either the 'db' or 'coll' keys,
     * then this will return null.
     */
    @BsonIgnore @Nullable
    public MongoNamespace getNamespace() {
        if (namespaceDocument == null) {
            return null;
        }
        if (!namespaceDocument.containsKey("db") || !namespaceDocument.containsKey("coll")) {
            return null;
        }

        return new MongoNamespace(namespaceDocument.getString("db").getValue(), namespaceDocument.getString("coll").getValue());
    }

    /**
     * Returns the namespaceDocument
     *
     * The namespaceDocument is a BsonDocument containing the values associated with a MongoNamespace. The
     * 'db' key refers to the database name and the 'coll' key refers to the collection name.
     *
     * @return the namespaceDocument
     * @since 3.8
     */
    @BsonProperty("ns")
    public BsonDocument getNamespaceDocument() {
        return namespaceDocument;
    }

    /**
     * Returns the database name
     *
     * @return the databaseName. If the namespaceDocument is null or if it is missing the 'db' key, then this will
     * return null.
     * @since 3.8
     */
    @BsonIgnore @Nullable
    public String getDatabaseName() {
        if (namespaceDocument == null) {
            return null;
        }
        if (!namespaceDocument.containsKey("db")) {
            return null;
        }
        return namespaceDocument.getString("db").getValue();
    }

    /**
     * Returns the fullDocument
     *
     * @return the fullDocument
     */
    @Nullable
    public TDocument getFullDocument() {
        return fullDocument;
    }

    /**
     * Returns a document containing just the _id of the changed document.
     * <p>
     * For unsharded collections this contains a single field, _id, with the
     * value of the _id of the document updated.  For sharded collections,
     * this will contain all the components of the shard key in order,
     * followed by the _id if the _id isn’t part of the shard key.
     * </p>
     *
     * @return the document key
     */
    public BsonDocument getDocumentKey() {
        return documentKey;
    }

    /**
     * Gets the cluster time at which the change occurred.
     *
     * @return the cluster time at which the change occurred
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    @Nullable
    public BsonTimestamp getClusterTime() {
        return clusterTime;
    }

    /**
     * Returns the operationType
     *
     * @return the operationType
     */
    public OperationType getOperationType() {
        return operationType;
    }

    /**
     * Returns the updateDescription
     *
     * @return the updateDescription
     */
    public UpdateDescription getUpdateDescription() {
        return updateDescription;
    }

    /**
     * Creates the codec for the specific ChangeStreamOutput type
     *
     * @param fullDocumentClass the class to use to represent the fullDocument
     * @param codecRegistry the codec registry
     * @param <TFullDocument> the fullDocument type
     * @return the codec
     */
    public static <TFullDocument> Codec<ChangeStreamDocument<TFullDocument>> createCodec(final Class<TFullDocument> fullDocumentClass,
                                                                                         final CodecRegistry codecRegistry) {
        return new ChangeStreamDocumentCodec<TFullDocument>(fullDocumentClass, codecRegistry);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChangeStreamDocument<?> that = (ChangeStreamDocument<?>) o;

        if (resumeToken != null ? !resumeToken.equals(that.resumeToken) : that.resumeToken != null) {
            return false;
        }
        if (namespaceDocument != null ? !namespaceDocument.equals(that.namespaceDocument) : that.namespaceDocument != null) {
            return false;
        }
        if (fullDocument != null ? !fullDocument.equals(that.fullDocument) : that.fullDocument != null) {
            return false;
        }
        if (documentKey != null ? !documentKey.equals(that.documentKey) : that.documentKey != null) {
            return false;
        }
        if (clusterTime != null ? !clusterTime.equals(that.clusterTime) : that.clusterTime != null) {
            return false;
        }
        if (operationType != that.operationType) {
            return false;
        }
        if (updateDescription != null ? !updateDescription.equals(that.updateDescription) : that.updateDescription != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = resumeToken != null ? resumeToken.hashCode() : 0;
        result = 31 * result + (namespaceDocument != null ? namespaceDocument.hashCode() : 0);
        result = 31 * result + (fullDocument != null ? fullDocument.hashCode() : 0);
        result = 31 * result + (documentKey != null ? documentKey.hashCode() : 0);
        result = 31 * result + (clusterTime != null ? clusterTime.hashCode() : 0);
        result = 31 * result + (operationType != null ? operationType.hashCode() : 0);
        result = 31 * result + (updateDescription != null ? updateDescription.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ChangeStreamDocument{"
                + "resumeToken=" + resumeToken
                + ", namespace=" + getNamespace()
                + ", fullDocument=" + fullDocument
                + ", documentKey=" + documentKey
                + ", clusterTime=" + clusterTime
                + ", operationType=" + operationType
                + ", updateDescription=" + updateDescription
                + "}";
    }
}
