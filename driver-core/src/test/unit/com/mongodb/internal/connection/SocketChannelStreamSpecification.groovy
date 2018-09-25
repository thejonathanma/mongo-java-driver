package com.mongodb.internal.connection

import com.mongodb.MongoSocketOpenException
import com.mongodb.ServerAddress
import com.mongodb.connection.BufferProvider
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import spock.lang.Specification

class SocketChannelStreamSpecification extends Specification {
    def 'should successfully connect with working ip address group'() {
        given:
        def port = 27017
        def socketSettings = SocketSettings.builder().build()
        def sslSettings = SslSettings.builder().build()
        def bufferProvider = Stub(BufferProvider)

        def inetAddresses = new InetSocketAddress[3]
        inetAddresses[0] = new InetSocketAddress(InetAddress.getByName('1.2.3.4'), port)
        inetAddresses[1] = new InetSocketAddress(InetAddress.getByName('2.3.4.5'), port)
        inetAddresses[2] = new InetSocketAddress(InetAddress.getByName('127.0.0.1'), port)

        def serverAddress = Stub(ServerAddress)
        serverAddress.getSocketAddresses() >> inetAddresses

        def socketChannelStream = new SocketChannelStream(serverAddress, socketSettings, sslSettings, bufferProvider)

        when:
        socketChannelStream.open()

        then:
        !socketChannelStream.isClosed()

        cleanup:
        socketChannelStream?.close()
    }

    def 'should throw exception when attempting connection with incorrect ip address group'() {
        given:
        def port = 27017
        def socketSettings = SocketSettings.builder().build()
        def sslSettings = SslSettings.builder().build()
        def bufferProvider = Stub(BufferProvider)

        def inetAddresses = new InetSocketAddress[3]
        inetAddresses[0] = new InetSocketAddress(InetAddress.getByName('1.2.3.4'), port)
        inetAddresses[1] = new InetSocketAddress(InetAddress.getByName('2.3.4.5'), port)
        inetAddresses[2] = new InetSocketAddress(InetAddress.getByName('1.2.3.5'), port)

        def serverAddress = Stub(ServerAddress)
        serverAddress.getSocketAddresses() >> inetAddresses

        def socketChannelStream = new SocketChannelStream(serverAddress, socketSettings, sslSettings, bufferProvider)

        when:
        socketChannelStream.open()

        then:
        thrown(MongoSocketOpenException)
        socketChannelStream.isClosed()

        cleanup:
        socketChannelStream?.close()
    }
}
