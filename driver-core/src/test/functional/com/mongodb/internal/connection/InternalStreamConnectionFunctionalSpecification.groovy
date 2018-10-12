package com.mongodb.internal.connection

import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.connection.ByteBufferBsonOutput
import com.mongodb.connection.ClusterConnectionMode
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SocketStreamFactory
import com.mongodb.connection.SslSettings
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import spock.lang.IgnoreIf

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.ClusterFixture.serverVersionAtLeast

class InternalStreamConnectionFunctionalSpecification extends OperationFunctionalSpecification {

    def serverId = new ServerId(new ClusterId(), new ServerAddress('localhost', 27017))
    def socketSettings = SocketSettings.builder().connectTimeout(1000, TimeUnit.MILLISECONDS).build()
    def sslSettings = SslSettings.builder().build()
    InternalStreamConnection connection = new InternalStreamConnection(serverId, new SocketStreamFactory(socketSettings, sslSettings), [],
            new TestCommandListener(), new InternalStreamConnectionInitializer([], null, []))

    def fieldNameValidator = new NoOpFieldNameValidator()
    def sessionContext = NoOpSessionContext.INSTANCE

    def findCommand = new BsonDocument('find', new BsonString(getCollectionName())).append('batchSize', new BsonInt32(1))
    def findMessage = new CommandMessage(namespace, findCommand, fieldNameValidator, ReadPreference.primary(),
            MessageSettings.builder().serverVersion(new ServerVersion([4, 1, 3])).serverType(ServerType.STANDALONE).build(),
            true, null, null, ClusterConnectionMode.SINGLE)

    @IgnoreIf({ !serverVersionAtLeast([4, 1, 3]) || getSslSettings().isEnabled() })
    def 'should call sendMessage only once when exhaust is set'() {
        given:
        def collectionHelper = getCollectionHelper()

        collectionHelper.insertDocuments([
                new BsonDocument('x', new BsonInt32(0)),
                new BsonDocument('x', new BsonInt32(1)),
                new BsonDocument('x', new BsonInt32(2))
        ])

        InternalStreamConnection connectionSpy = Spy(InternalStreamConnection,
                constructorArgs:[serverId, new SocketStreamFactory(socketSettings, sslSettings), [],
                                 new TestCommandListener(), new InternalStreamConnectionInitializer([], null, [])])
        connectionSpy.open()

        BsonDocument findResult = connectionSpy.sendAndReceive(findMessage, new BsonDocumentCodec(), sessionContext)
        def bsonOutput = new ByteBufferBsonOutput(connectionSpy)

        def getMoreCommand = new BsonDocument('getMore', findResult.getDocument('cursor').getInt64('id'))
                .append('collection', new BsonString(getCollectionName()))
                .append('batchSize', new BsonInt32(1))
        def getMoreMessage = new CommandMessage(namespace, getMoreCommand, fieldNameValidator, ReadPreference.primary(),
                MessageSettings.builder().serverVersion(new ServerVersion([4, 1, 3])).serverType(ServerType.STANDALONE).build(),
                true, null, null, ClusterConnectionMode.SINGLE, true)

        getMoreMessage.encode(bsonOutput, sessionContext)

        when:
        for (int i = 0; i < 3; i++) {
            connectionSpy.sendAndReceive(getMoreMessage, new BsonDocumentCodec(), sessionContext)
        }

        then:
        1 * connectionSpy.sendMessage(*_)

        cleanup:
        connection?.close()
    }
}
