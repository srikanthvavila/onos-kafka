/*
 * Copyright 2016 Open Networking Lab
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
package org.ciena.onos.link_event_publisher;

import java.util.HashMap;
import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onosproject.mastership.MastershipService;
import org.onosproject.net.link.LinkEvent;
import org.onosproject.net.link.LinkListener;
import org.onosproject.net.link.LinkService;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.ciena.onos.ext_notifier.api.PublisherRegistry;
import org.ciena.onos.ext_notifier.api.PublisherSource;
import org.ciena.onos.ext_notifier.api.Notifier;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * Publisher which publishes link events to external systems.
 */
@Component(immediate = true)
public class LinkEventPublisher implements PublisherSource {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final static String publishes = "link.events";

    private LinkListener linkListener = null;

    // For subscribing to device-related events
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected LinkService linkService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected MastershipService mastershipService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PublisherRegistry<PublisherSource> publisherRegistry;

    @Activate
    protected void activate(ComponentContext context) {
        publisherRegistry.register(this);
        log.info("Activated");
    }

    @Deactivate
    protected void deactivate() {
        stop();
        log.info("Stopped");
    }

    @Override
    public String publishes() {
        return publishes;
    }

    /**
     * Marshal a {@link org.onosproject.net.link.LinkEvent} to a stringified
     * JSON object.
     *
     * @param event
     *            the link event to marshal
     * @return stringified JSON encoding of the link event
     */
    private String encodeEvent(LinkEvent event, String userData) {
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        builder.append(String.format("\"type\":\"%s\",", event.type().toString()));
        builder.append(String.format("\"time\":%d,", event.time()));
        builder.append(String.format("\"src\":\"%s\",", event.subject().src().deviceId().toString()));
        builder.append(String.format("\"src-port\":%d,", event.subject().src().port().toLong()));
        builder.append(String.format("\"dst\":\"%s\",", event.subject().dst().deviceId().toString()));
        builder.append(String.format("\"dst-port\":%d,", event.subject().dst().port().toLong()));
        builder.append(String.format("\"state\":\"%s\",", event.subject().state()));
        builder.append(String.format("\"durable\":%s", event.subject().isDurable()));
        if (userData != null) {
            Map<String, String> userDataMap = new Gson().fromJson(userData,
                    new TypeToken<HashMap<String, String>>() {
                    }.getType());
            if (userDataMap != null) {
                userDataMap.keySet().forEach((key) -> {
                    builder.append(String.format(",\"%s\":\"%s\"", key, userDataMap.get(key)));
                });
            }
        }
        builder.append('}');
        return builder.toString();
    }

    @Override
    public void start(Notifier notifier, String userData) {
        if (linkService != null) {
            linkListener = new LinkListener() {

                @Override
                public void event(LinkEvent event) {

                    /*
                     * Only publish events if the destination of the link is
                     * mastered by the current instance so that we get some load
                     * balancing across instances in a clustered environment.
                     */
                    if (mastershipService.isLocalMaster(event.subject().dst().deviceId())) {
                        String encoded = encodeEvent(event, userData);
                        notifier.publish(encoded);
                    } else {
                        log.debug("DROPPING LINK EVENT: not local master: {}",
                                mastershipService.getMasterFor(event.subject().src().deviceId()));
                    }
                }
            };

            linkService.addListener(linkListener);
            log.info("Added link notification listener for Kafka bridge");
        } else {
            log.error(
                    "Unable to resolve link service and add link listener, no link events will be published on Kafka bridge");
        }
    }

    @Override
    public void stop() {
        if (linkListener != null) {
            linkService.removeListener(linkListener);
            log.info("Removing link listener for Kafka bridge");
            linkListener = null;
        }
    }
}
