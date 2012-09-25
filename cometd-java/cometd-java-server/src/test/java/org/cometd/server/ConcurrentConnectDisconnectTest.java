/*
 * Copyright (c) 2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cometd.server;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;

import org.cometd.bayeux.Channel;
import org.cometd.bayeux.server.ServerChannel;
import org.cometd.bayeux.server.ServerMessage;
import org.cometd.bayeux.server.ServerSession;
import org.cometd.server.transport.JSONTransport;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpExchange;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.Assert;
import org.junit.Test;

public class ConcurrentConnectDisconnectTest extends AbstractBayeuxClientServerTest
{
    @Test
    public void testConnectListenerThenDisconnectThenConnectHandler() throws Exception
    {
        final CountDownLatch connectLatch = new CountDownLatch(2);
        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        bayeux.getChannel("/meta/connect").addListener(new ServerChannel.MessageListener()
        {
            public boolean onMessage(ServerSession from, ServerChannel channel, ServerMessage.Mutable message)
            {
                connectLatch.countDown();
                if (connectLatch.getCount() == 0)
                    await(disconnectLatch);
                return true;
            }
        });

        ContentExchange handshake = newBayeuxExchange("[{" +
                "\"channel\": \"/meta/handshake\"," +
                "\"version\": \"1.0\"," +
                "\"minimumVersion\": \"1.0\"," +
                "\"supportedConnectionTypes\": [\"long-polling\"]" +
                "}]");
        httpClient.send(handshake);
        Assert.assertEquals(HttpExchange.STATUS_COMPLETED, handshake.waitForDone());
        Assert.assertEquals(200, handshake.getResponseStatus());

        String clientId = extractClientId(handshake);
        String bayeuxCookie = extractBayeuxCookie(handshake);

        String channelName = "/foo";
        ContentExchange subscribe = newBayeuxExchange("[{" +
                "\"clientId\": \"" + clientId + "\"," +
                "\"channel\": \"/meta/subscribe\"," +
                "\"subscription\": \"" + channelName + "\"" +
                "}]");
        httpClient.send(subscribe);
        Assert.assertEquals(HttpExchange.STATUS_COMPLETED, subscribe.waitForDone());
        Assert.assertEquals(200, subscribe.getResponseStatus());

        ContentExchange connect1 = newBayeuxExchange("[{" +
                "\"channel\": \"/meta/connect\"," +
                "\"clientId\": \"" + clientId + "\"," +
                "\"connectionType\": \"long-polling\"" +
                "}]");
        connect1.setRequestHeader(HttpHeaders.COOKIE, bayeuxCookie);
        httpClient.send(connect1);
        Assert.assertEquals(HttpExchange.STATUS_COMPLETED, connect1.waitForDone());
        Assert.assertEquals(200, connect1.getResponseStatus());

        ContentExchange connect2 = newBayeuxExchange("[{" +
                "\"channel\": \"/meta/connect\"," +
                "\"clientId\": \"" + clientId + "\"," +
                "\"connectionType\": \"long-polling\"" +
                "}]");
        connect2.setRequestHeader(HttpHeaders.COOKIE, bayeuxCookie);
        httpClient.send(connect2);

        // Wait for the second connect to arrive, then disconnect
        Assert.assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

        ContentExchange disconnect = newBayeuxExchange("[{" +
                "\"channel\": \"/meta/disconnect\"," +
                "\"clientId\": \"" + clientId + "\"" +
                "}]");
        httpClient.send(disconnect);
        Assert.assertEquals(HttpExchange.STATUS_COMPLETED, disconnect.waitForDone());
        Assert.assertEquals(200, disconnect.getResponseStatus());

        disconnectLatch.countDown();

        Assert.assertEquals(HttpExchange.STATUS_COMPLETED, connect2.waitForDone());
        Assert.assertEquals(200, connect2.getResponseStatus());

        Assert.assertTrue(connect2.getResponseContent().toLowerCase().contains("unknown"));

        Assert.assertNull(bayeux.getSession(clientId));
    }

    @Test
    public void testConnectHandlerThenDisconnect() throws Exception
    {
        final CountDownLatch connectLatch = new CountDownLatch(2);
        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch suspendLatch = new CountDownLatch(1);
        bayeux.setTransports(new JSONTransport(bayeux)
        {
            {
                init();
            }

            @Override
            protected ServerMessage.Mutable bayeuxServerHandle(ServerSessionImpl session, ServerMessage.Mutable message)
            {
                ServerMessage.Mutable reply = super.bayeuxServerHandle(session, message);
                if (Channel.META_CONNECT.equals(message.getChannel()))
                {
                    connectLatch.countDown();
                    if (connectLatch.getCount() == 0)
                        await(disconnectLatch);
                }
                return reply;
            }

            @Override
            protected void metaConnectSuspended(HttpServletRequest request, ServerSession session, long timeout)
            {
                suspendLatch.countDown();
            }
        });

        ContentExchange handshake = newBayeuxExchange("[{" +
                "\"channel\": \"/meta/handshake\"," +
                "\"version\": \"1.0\"," +
                "\"minimumVersion\": \"1.0\"," +
                "\"supportedConnectionTypes\": [\"long-polling\"]" +
                "}]");
        httpClient.send(handshake);
        Assert.assertEquals(HttpExchange.STATUS_COMPLETED, handshake.waitForDone());
        Assert.assertEquals(200, handshake.getResponseStatus());

        String clientId = extractClientId(handshake);
        String bayeuxCookie = extractBayeuxCookie(handshake);

        String channelName = "/foo";
        ContentExchange subscribe = newBayeuxExchange("[{" +
                "\"clientId\": \"" + clientId + "\"," +
                "\"channel\": \"/meta/subscribe\"," +
                "\"subscription\": \"" + channelName + "\"" +
                "}]");
        httpClient.send(subscribe);
        Assert.assertEquals(HttpExchange.STATUS_COMPLETED, subscribe.waitForDone());
        Assert.assertEquals(200, subscribe.getResponseStatus());

        ContentExchange connect1 = newBayeuxExchange("[{" +
                "\"channel\": \"/meta/connect\"," +
                "\"clientId\": \"" + clientId + "\"," +
                "\"connectionType\": \"long-polling\"" +
                "}]");
        connect1.setRequestHeader(HttpHeaders.COOKIE, bayeuxCookie);
        httpClient.send(connect1);
        Assert.assertEquals(HttpExchange.STATUS_COMPLETED, connect1.waitForDone());
        Assert.assertEquals(200, connect1.getResponseStatus());

        ContentExchange connect2 = newBayeuxExchange("[{" +
                "\"channel\": \"/meta/connect\"," +
                "\"clientId\": \"" + clientId + "\"," +
                "\"connectionType\": \"long-polling\"" +
                "}]");
        connect2.setRequestHeader(HttpHeaders.COOKIE, bayeuxCookie);
        httpClient.send(connect2);

        // Wait for the second connect to arrive, then disconnect
        Assert.assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

        ContentExchange disconnect = newBayeuxExchange("[{" +
                "\"channel\": \"/meta/disconnect\"," +
                "\"clientId\": \"" + clientId + "\"" +
                "}]");
        httpClient.send(disconnect);
        Assert.assertEquals(HttpExchange.STATUS_COMPLETED, disconnect.waitForDone());
        Assert.assertEquals(200, disconnect.getResponseStatus());

        disconnectLatch.countDown();

        // The connect must not be suspended
        Assert.assertFalse(suspendLatch.await(1, TimeUnit.SECONDS));

        Assert.assertEquals(HttpExchange.STATUS_COMPLETED, connect2.waitForDone());
        Assert.assertEquals(200, connect2.getResponseStatus());

        Assert.assertTrue(connect2.getResponseContent().toLowerCase().contains("\"none\""));

        Assert.assertNull(bayeux.getSession(clientId));
    }

    private void await(CountDownLatch latch)
    {
        try
        {
            latch.await();
        }
        catch (InterruptedException x)
        {
            Thread.currentThread().interrupt();
        }
    }
}
