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
package org.ciena.onos.link_event_publisher;

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
import org.onosproject.net.link.LinkListener;
import org.onosproject.net.link.LinkService;

/**
 * Set of tests for the ONOS device event publisher component.
 */
public class LinkEventPublisherTest {
    protected PublisherRegistry mockPublisherRegistry;
    protected LinkService mockLinkService;
    protected ControllerNode mockControllerNode;
    protected NodeId mockNodeId;
    protected MastershipService mockMastershipService;

    private LinkEventPublisher componentLinkPublisher;

    @Before
    public void setUpMocks() {
        mockPublisherRegistry = EasyMock.createMock(PublisherRegistry.class);
        mockPublisherRegistry.register(isA(PublisherSource.class));
        EasyMock.expectLastCall().once();
        mockPublisherRegistry.unregister(isA(PublisherSource.class));
        EasyMock.expectLastCall().once();

        mockLinkService = EasyMock.createMock(LinkService.class);
        mockLinkService.addListener(isA(LinkListener.class));
        EasyMock.expectLastCall().once();
        mockLinkService.removeListener(isA(LinkListener.class));
        EasyMock.expectLastCall().once();

        mockMastershipService = EasyMock.createMock(MastershipService.class);
        EasyMock.expect(mockMastershipService.isLocalMaster(isA(DeviceId.class))).andReturn(true).anyTimes();

        mockNodeId = new NodeId("sample-id");
        mockControllerNode = EasyMock.createMock(ControllerNode.class);
        EasyMock.expect(mockControllerNode.id()).andReturn(mockNodeId).anyTimes();
    }

    @Test
    public void basics() {
        EasyMock.replay(mockLinkService);
        EasyMock.replay(mockMastershipService);
        EasyMock.replay(mockControllerNode);
        EasyMock.replay(mockPublisherRegistry);
        componentLinkPublisher = new LinkEventPublisher();

        componentLinkPublisher.linkService = mockLinkService;
        componentLinkPublisher.mastershipService = mockMastershipService;
        componentLinkPublisher.publisherRegistry = mockPublisherRegistry;

        componentLinkPublisher.activate(null);
        componentLinkPublisher.start(null,null);
        componentLinkPublisher.stop();
    }

    /**
     * Verifies test mocks.
     */
    @After
    public void tearDownMocks() {
        componentLinkPublisher.deactivate();
        EasyMock.verify(mockLinkService);
    }

}

