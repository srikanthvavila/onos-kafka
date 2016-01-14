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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.onosproject.olt.AccessDeviceService;

/**
 * Publisher which publishes VOlt application events to external systems.
 */
@Component(immediate = true)
public class VOLTEventPublisher implements PublisherSource {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final static String publishes = "volt.events";

    // For subscribing to device-related events
    //@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    //protected AccessDeviceService oltService;

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
	private String encodeEvent() {
		return null;//TODO
	}

	@Override
	public void start(Notifier notifier) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}
}
