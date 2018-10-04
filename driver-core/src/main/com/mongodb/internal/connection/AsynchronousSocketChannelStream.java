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

package com.mongodb.internal.connection;

import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.Stream;
import org.bson.ByteBuf;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class AsynchronousSocketChannelStream implements Stream {
    private final ServerAddress serverAddress;
    private final SocketSettings settings;
    private final BufferProvider bufferProvider;
    private final AsynchronousChannelGroup group;
    private volatile AsynchronousSocketChannel channel;
    private volatile boolean isClosed;

    public AsynchronousSocketChannelStream(final ServerAddress serverAddress, final SocketSettings settings,
                                    final BufferProvider bufferProvider, final AsynchronousChannelGroup group) {
        this.serverAddress = serverAddress;
        this.settings = settings;
        this.bufferProvider = bufferProvider;
        this.group = group;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return bufferProvider.getBuffer(size);
    }

    @Override
    public void open() throws IOException {
        FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<Void>();
        openAsync(handler);
        handler.getOpen();
    }

    @Override
    public void openAsync(final AsyncCompletionHandler<Void> handler) {
        isTrue("unopened", channel == null);
        initializeSocketChannel(handler, new LinkedList<SocketAddress>(serverAddress.getSocketAddresses()));
    }

    @SuppressWarnings("deprecation")
    private void initializeSocketChannel(final AsyncCompletionHandler<Void> handler, final Queue<SocketAddress> socketAddressQueue) {
        if (socketAddressQueue.isEmpty()) {
            handler.failed(new MongoSocketException("Exception opening socket", serverAddress));
        } else {
            SocketAddress socketAddress = socketAddressQueue.poll();

            try {
                AsynchronousSocketChannel attemptConnectionChannel = AsynchronousSocketChannel.open(group);
                attemptConnectionChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                attemptConnectionChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, settings.isKeepAlive());
                if (settings.getReceiveBufferSize() > 0) {
                    attemptConnectionChannel.setOption(StandardSocketOptions.SO_RCVBUF, settings.getReceiveBufferSize());
                }
                if (settings.getSendBufferSize() > 0) {
                    attemptConnectionChannel.setOption(StandardSocketOptions.SO_SNDBUF, settings.getSendBufferSize());
                }

                attemptConnectionChannel.connect(socketAddress, null,
                        new OpenCompletionHandler(handler, socketAddressQueue, attemptConnectionChannel));
            } catch (IOException e) {
                handler.failed(new MongoSocketOpenException("Exception opening socket", serverAddress, e));
            } catch (Throwable t) {
                handler.failed(t);
            }
        }
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<Void>();
        writeAsync(buffers, handler);
        handler.getWrite();
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        FutureAsyncCompletionHandler<ByteBuf> handler = new FutureAsyncCompletionHandler<ByteBuf>();
        readAsync(numBytes, handler);
        return handler.getRead();
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        final AsyncWritableByteChannel byteChannel = new AsyncWritableByteChannelAdapter();
        final Iterator<ByteBuf> iter = buffers.iterator();
        pipeOneBuffer(byteChannel, iter.next(), new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void t) {
                if (iter.hasNext()) {
                    pipeOneBuffer(byteChannel, iter.next(), this);
                } else {
                    handler.completed(null);
                }
            }

            @Override
            public void failed(final Throwable t) {
                handler.failed(t);
            }
        });
    }

    @Override
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        ByteBuf buffer = bufferProvider.getBuffer(numBytes);
        channel.read(buffer.asNIO(), settings.getReadTimeout(MILLISECONDS), MILLISECONDS, null,
                     new BasicCompletionHandler(buffer, handler));
    }

    @Override
    public ServerAddress getAddress() {
        return serverAddress;
    }

    /**
     * Closes the connection.
     */
    @Override
    public void close() {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) { // NOPMD
            // ignore
        } finally {
            channel = null;
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public SocketSettings getSettings() {
        return settings;
    }

    public AsynchronousChannelGroup getGroup() {
        return group;
    }

    private void pipeOneBuffer(final AsyncWritableByteChannel byteChannel, final ByteBuf byteBuffer,
                               final AsyncCompletionHandler<Void> outerHandler) {
        byteChannel.write(byteBuffer.asNIO(), new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void t) {
                if (byteBuffer.hasRemaining()) {
                    byteChannel.write(byteBuffer.asNIO(), this);
                } else {
                    outerHandler.completed(null);
                }
            }

            @Override
            public void failed(final Throwable t) {
                outerHandler.failed(t);
            }
        });
    }

    private class AsyncWritableByteChannelAdapter implements AsyncWritableByteChannel {
        @Override
        public void write(final ByteBuffer src, final AsyncCompletionHandler<Void> handler) {
            channel.write(src, null, new WriteCompletionHandler(handler));
        }

        private class WriteCompletionHandler extends BaseCompletionHandler<Void, Integer, Object> {

            WriteCompletionHandler(final AsyncCompletionHandler<Void> handler) {
                super(handler);
            }

            @Override
            public void completed(final Integer result, final Object attachment) {
                AsyncCompletionHandler<Void> localHandler = getHandlerAndClear();
                localHandler.completed(null);
            }

            @Override
            public void failed(final Throwable exc, final Object attachment) {
                AsyncCompletionHandler<Void> localHandler = getHandlerAndClear();
                localHandler.failed(exc);
            }
        }
    }

    private final class BasicCompletionHandler extends BaseCompletionHandler<ByteBuf, Integer, Void> {
        private final AtomicReference<ByteBuf> byteBufReference;

        private BasicCompletionHandler(final ByteBuf dst, final AsyncCompletionHandler<ByteBuf> handler) {
            super(handler);
            this.byteBufReference = new AtomicReference<ByteBuf>(dst);
        }

        @Override
        public void completed(final Integer result, final Void attachment) {
            AsyncCompletionHandler<ByteBuf> localHandler = getHandlerAndClear();
            ByteBuf localByteBuf = byteBufReference.getAndSet(null);
            if (result == -1) {
                localByteBuf.release();
                localHandler.failed(new MongoSocketReadException("Prematurely reached end of stream", serverAddress));
            } else if (!localByteBuf.hasRemaining()) {
                localByteBuf.flip();
                localHandler.completed(localByteBuf);
            } else {
                channel.read(localByteBuf.asNIO(), settings.getReadTimeout(MILLISECONDS), MILLISECONDS, null,
                             new BasicCompletionHandler(localByteBuf, localHandler));
            }
        }

        @Override
        public void failed(final Throwable t, final Void attachment) {
            AsyncCompletionHandler<ByteBuf> localHandler = getHandlerAndClear();
            ByteBuf localByteBuf = byteBufReference.getAndSet(null);
            localByteBuf.release();
            if (t instanceof InterruptedByTimeoutException) {
                localHandler.failed(new MongoSocketReadTimeoutException("Timeout while receiving message", serverAddress, t));
            } else {
                localHandler.failed(t);
            }
        }
    }

    private class OpenCompletionHandler extends BaseCompletionHandler<Void, Void, Object> {
        private final Queue<SocketAddress> socketAddressQueue;
        private final AsynchronousSocketChannel attemptConnectionChannel;

        OpenCompletionHandler(final AsyncCompletionHandler<Void> handler, final Queue<SocketAddress> socketAddressQueue,
                              final AsynchronousSocketChannel attemptConnectionChannel) {
            super(handler);
            this.socketAddressQueue = socketAddressQueue;
            this.attemptConnectionChannel = attemptConnectionChannel;
        }

        @Override
        public void completed(final Void result, final Object attachment) {
            channel = attemptConnectionChannel;
            AsyncCompletionHandler<Void> localHandler = getHandlerAndClear();
            localHandler.completed(null);
        }

        @Override
        public void failed(final Throwable exc, final Object attachment) {
            AsyncCompletionHandler<Void> localHandler = getHandlerAndClear();

            if (socketAddressQueue.isEmpty()) {
                if (exc instanceof IOException) {
                    localHandler.failed(new MongoSocketOpenException("Exception opening socket", getAddress(), exc));
                } else {
                    localHandler.failed(exc);
                }
            } else {
                initializeSocketChannel(localHandler, socketAddressQueue);
            }
        }
    }

    // Private base class for all CompletionHandler implementors that ensures the upstream handler is
    // set to null before it is used.  This is to work around an observed issue with implementations of
    // AsynchronousSocketChannel that fail to clear references to handlers stored in instance fields of
    // the class.
    private abstract static class BaseCompletionHandler<T, V, A> implements CompletionHandler<V, A> {
        private final AtomicReference<AsyncCompletionHandler<T>> handlerReference;

        BaseCompletionHandler(final AsyncCompletionHandler<T> handler) {
            this.handlerReference = new AtomicReference<AsyncCompletionHandler<T>>(handler);
        }

        protected AsyncCompletionHandler<T> getHandlerAndClear() {
            return handlerReference.getAndSet(null);
        }
    }
}
