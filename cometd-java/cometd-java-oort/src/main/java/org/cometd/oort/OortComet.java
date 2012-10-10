/*
 * Copyright (c) 2010 the original author or authors.
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

package org.cometd.oort;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;
import org.cometd.websocket.client.WebSocketTransport;

/**
 * <p>The Oort comet client connects a local Oort comet server to a remote Oort comet server.</p>
 * <p>
 */
public class OortComet extends BayeuxClient
{
    private final ConcurrentMap<String, ClientSessionChannel.MessageListener> _subscriptions = new ConcurrentHashMap<>();
    private final Oort _oort;
    private final String _cometURL;
    private volatile boolean _subscriptionsAllowed;

    public OortComet(Oort oort, String cometURL)
    {
        super(cometURL,WebSocketTransport.create(null,oort.getWebSocketClientFactory()),LongPollingTransport.create(null, oort.getHttpClient()));
        _oort = oort;
        _cometURL = cometURL;
        setDebugEnabled(oort.isClientDebugEnabled());
        // Add listener for handshake response
        getChannel(Channel.META_HANDSHAKE).addListener(new HandshakeListener());
    }

    protected void subscribe(Set<String> observedChannels)
    {
        // Guard against concurrent subscription clearing from the handshake callback
        if (!_subscriptionsAllowed)
            return;

        for (String channel : observedChannels)
        {
            if (_subscriptions.containsKey(channel))
                continue;

            ClientSessionChannel.MessageListener listener = new ClientSessionChannel.MessageListener()
            {
                public void onMessage(ClientSessionChannel channel, Message message)
                {
                    debug("Republishing message {} from {}", message, _cometURL);
                    // BayeuxServer may sweep channels, so calling bayeux.getChannel(...)
                    // may return null, and therefore we use the client to send the message
                    _oort.getOortSession().getChannel(message.getChannel()).publish(message.getData());
                }
            };

            ClientSessionChannel.MessageListener existing = _subscriptions.putIfAbsent(channel, listener);
            if (existing == null)
            {
                debug("Subscribing to messages on {} from {}", channel, _cometURL);
                getChannel(channel).subscribe(listener);
            }
        }
        debug("Subscriptions to messages on {} from {}", _subscriptions, _cometURL);
    }

    protected void unsubscribe(String channel)
    {
        ClientSessionChannel.MessageListener listener = _subscriptions.remove(channel);
        if (listener != null)
        {
            debug("Unsubscribing to messages on {} from {}", channel, _cometURL);
            getChannel(channel).unsubscribe(listener);
        }
    }

    protected void clearSubscriptions()
    {
        for (String channel : _oort.getObservedChannels())
            unsubscribe(channel);
    }

    @Override
    public String toString()
    {
        return _cometURL + "@" + getId();
    }

    private class HandshakeListener implements ClientSessionChannel.MessageListener
    {
        public void onMessage(ClientSessionChannel channel, Message message)
        {
            if (!message.isSuccessful())
                return;

            Map<String,Object> ext = message.getExt();
            if (ext == null)
                return;

            Object oortExtObject = ext.get(Oort.EXT_OORT_FIELD);
            if (!(oortExtObject instanceof Map))
                return;

            batch(new Runnable()
            {
                public void run()
                {
                    // Subscribe to cloud notifications
                    getChannel(Oort.OORT_CLOUD_CHANNEL).subscribe(new ClientSessionChannel.MessageListener()
                    {
                        public void onMessage(ClientSessionChannel channel, Message message)
                        {
                            if (message.isSuccessful())
                                _oort.joinComets(message);
                        }
                    });

                    // It is possible that a call to Oort.observeChannel() (which triggers
                    // the call to subscribe()) is performed concurrently with the handshake
                    // of this OortComet with a remote comet.
                    // For example, Seti calls Oort.observeChannel() on startup and this may
                    // be called while the Oort cloud is connecting all the comets together.
                    // In this case, below we will clear existing subscriptions, but we will
                    // subscribe them again just afterwards, ensuring only one subscriber
                    // (and not multiple ones) is subscribed.
                    clearSubscriptions();
                    _subscriptionsAllowed = true;
                    subscribe(_oort.getObservedChannels());

                    getChannel(Oort.OORT_CLOUD_CHANNEL).publish(new ArrayList<>(_oort.getKnownComets()));
                }
            });
        }
    }
}
