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
package org.ciena.onos;

import java.util.HashMap;
import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onosproject.mastership.MastershipService;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceEvent.Type;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * Publisher which publishes device events to external systems.
 */
@Component(immediate = true)
public class DeviceEventPublisher implements PublisherSource {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final static String publishes = "device.events";
    private DeviceListener deviceListener = null;

    // For subscribing to device-related events
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

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
     * Marshal a {@link org.onosproject.net.device.DeviceEvent} to a stringified
     * JSON object.
     *
     * @param event
     *            the device event to marshal
     * @return stringified JSON encoding of the device event
     */
    private String encodeEvent(DeviceEvent event, String userData) {
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        builder.append(String.format("\"type\":\"%s\",", event.type().toString()));
        builder.append(String.format("\"time\":%d,", event.time()));
        builder.append("\"subject\":{");
        builder.append(String.format("\"id\":\"%s\",", event.subject().id()));
        builder.append(String.format("\"chassis\":\"%s\",", event.subject().chassisId().toString()));
        builder.append(String.format("\"hw-version\":\"%s\",", event.subject().hwVersion()));
        builder.append(String.format("\"manufacturer\":\"%s\",", event.subject().manufacturer()));
        builder.append(String.format("\"serial-number\":\"%s\",", event.subject().serialNumber()));
        builder.append(String.format("\"sw-version\":\"%s\",", event.subject().swVersion()));
        builder.append(String.format("\"provider\":\"%s\",", event.subject().providerId().id()));
        builder.append(String.format("\"type\":\"%s\"", event.subject().type()));
        builder.append('}');
        if (event.port() != null) {
            builder.append(",\"port\":{");
            builder.append(String.format("\"type\":\"%s\",", event.port().type()));
            builder.append(String.format("\"number\":%d,", event.port().number().toLong()));
            builder.append(String.format("\"speed\":%d,", event.port().portSpeed()));
            builder.append(String.format("\"enabled\":%s", event.port().isEnabled()));
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
        }
        builder.append('}');
        return builder.toString();
    }

    @Override
    public void start(Notifier notifier, String userData) {
        if (deviceService != null) {
            deviceListener = new DeviceListener() {
                @Override
                public void event(DeviceEvent event) {

                    /*
                     * Filtering out PPRT_STAT events (too many of them) and
                     * also only publishing if the subject of the event is
                     * mastered by the current instance so that we get some load
                     * balancing across instances in a clustered environment.
                     */
                    if (event.type() != Type.PORT_STATS_UPDATED) {
                        if (mastershipService.isLocalMaster(event.subject().id())) {
                            String encoded = encodeEvent(event, userData);
                            notifier.publish(encoded);
                        } else {
                            log.debug("DROPPING DEVICE EVENT: not local master: {}",
                                    mastershipService.getMasterFor(event.subject().id()));
                        }
                    } else {
                        log.debug("Filtering DEVICE PORT_STATS_UPDATED EVENT");
                    }
                }
            };

            deviceService.addListener(deviceListener);
            log.info("Added device notification listener for Kafka bridge");
        } else {
            log.error(
                    "Unable to resolve device service and add device listener, no device events will be published on Kafka bridge");
        }
    }

    @Override
    public void stop() {
        if (deviceListener != null) {
            log.info("Removing device listener for Kafka bridge");
            deviceService.removeListener(deviceListener);
            deviceListener = null;
        }
    }

}