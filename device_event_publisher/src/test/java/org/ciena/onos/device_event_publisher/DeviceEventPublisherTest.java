/*
 * Copyright 2016 ON.Lab Networks
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
package org.ciena.onos.device_event_publisher;

import static org.easymock.EasyMock.isA;

import org.ciena.onos.ext_notifier.api.PublisherRegistry;
import org.ciena.onos.ext_notifier.api.PublisherSource;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.cluster.NodeId;
import org.onosproject.mastership.MastershipService;
import org.onosproject.net.DeviceId;
import org.onosproject.net.MastershipRole;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;

/**
 * Set of tests for the ONOS device event publisher component.
 */
public class DeviceEventPublisherTest {
    protected PublisherRegistry mockPublisherRegistry;
    protected DeviceService mockDeviceService;
    protected ControllerNode mockControllerNode;
    protected NodeId mockNodeId;
    protected MastershipService mockMastershipService;

    private DeviceEventPublisher componentDevicePublisher;

    @Before
    public void setUpMocks() {
        mockPublisherRegistry = EasyMock.createMock(PublisherRegistry.class);
        mockPublisherRegistry.register(isA(PublisherSource.class));
        EasyMock.expectLastCall().once();
        mockPublisherRegistry.unregister(isA(PublisherSource.class));
        EasyMock.expectLastCall().once();

        mockDeviceService = EasyMock.createMock(DeviceService.class);
        EasyMock.expect(mockDeviceService.isAvailable(isA(DeviceId.class))).andReturn(true).anyTimes();
        EasyMock.expect(mockDeviceService.getRole(isA(DeviceId.class))).andReturn(MastershipRole.MASTER).anyTimes();
        mockDeviceService.addListener(isA(DeviceListener.class));
        EasyMock.expectLastCall().once();
        mockDeviceService.removeListener(isA(DeviceListener.class));
        EasyMock.expectLastCall().once();

        mockMastershipService = EasyMock.createMock(MastershipService.class);
        EasyMock.expect(mockMastershipService.isLocalMaster(isA(DeviceId.class))).andReturn(true).anyTimes();

        mockNodeId = new NodeId("sample-id");
        mockControllerNode = EasyMock.createMock(ControllerNode.class);
        EasyMock.expect(mockControllerNode.id()).andReturn(mockNodeId).anyTimes();
    }

    @Test
    public void basics() {
        EasyMock.replay(mockDeviceService);
        EasyMock.replay(mockMastershipService);
        EasyMock.replay(mockControllerNode);
        EasyMock.replay(mockPublisherRegistry);
        componentDevicePublisher = new DeviceEventPublisher();

        componentDevicePublisher.deviceService = mockDeviceService;
        componentDevicePublisher.mastershipService = mockMastershipService;
        componentDevicePublisher.publisherRegistry = mockPublisherRegistry;

        componentDevicePublisher.activate(null);
        componentDevicePublisher.start(null,null);
        componentDevicePublisher.stop();
    }

    /**
     * Verifies test mocks.
     */
    @After
    public void tearDownMocks() {
        componentDevicePublisher.deactivate();
        EasyMock.verify(mockDeviceService);
    }

}

