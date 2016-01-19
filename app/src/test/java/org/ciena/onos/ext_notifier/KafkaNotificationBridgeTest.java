/*
 * Copyright 2015 Ciena Networks
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
package org.ciena.onos.ext_notifier;

import static org.easymock.EasyMock.isA;

import java.util.HashSet;
import java.util.Set;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.cfg.ConfigProperty;
import org.onosproject.cluster.ClusterService;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.cluster.NodeId;
import org.osgi.service.component.ComponentContext;

import org.ciena.onos.ext_notifier.api.PublisherSource;

/**
 * Set of tests for the ONOS external notifier component.
 */
public class KafkaNotificationBridgeTest {

    private KafkaNotificationBridge componentKafka;
    //private LinkEventPublisher componentLinkPublisher;

    protected PublisherSource mockPublisher;
    protected ComponentConfigService mockConfigService;
    protected ClusterService mockClusterService;
    protected ControllerNode mockControllerNode;
    protected ComponentContext mockContext;
    protected NodeId mockNodeId;

    private static final String MOCK_PUBLISHER_ID = "mock.events";

    @Before
    public void setUpMocks() {
        mockPublisher = EasyMock.createMock(PublisherSource.class);
        EasyMock.expect(mockPublisher.publishes()).andReturn(MOCK_PUBLISHER_ID).anyTimes();

        mockConfigService = EasyMock.createMock(ComponentConfigService.class);
        Set<ConfigProperty> config = new HashSet<ConfigProperty>();
        EasyMock.expect(mockConfigService.getProperties(isA(String.class))).andReturn(config).anyTimes();
        mockConfigService.registerProperties(isA(Class.class));
        EasyMock.expectLastCall().once();
        mockConfigService.unregisterProperties(isA(Class.class), EasyMock.anyBoolean());
        EasyMock.expectLastCall().once();

        mockNodeId = new NodeId("sample-id");
        mockControllerNode = EasyMock.createMock(ControllerNode.class);
        EasyMock.expect(mockControllerNode.id()).andReturn(mockNodeId).anyTimes();

        mockClusterService = EasyMock.createMock(ClusterService.class);
        EasyMock.expect(mockClusterService.getLocalNode()).andReturn(mockControllerNode).anyTimes();
    }

    /**
     * Verifies test mocks.
     */
    @After
    public void tearDownMocks() {
        componentKafka.deactivate();
        //EasyMock.verify(mockDeviceService);
    }

    @Test
    public void basics() {
        EasyMock.replay(mockConfigService);
        EasyMock.replay(mockClusterService);
        EasyMock.replay(mockControllerNode);
        EasyMock.replay(mockPublisher);
        componentKafka = new KafkaNotificationBridge();
        componentKafka.configService = mockConfigService;
        componentKafka.clusterService = mockClusterService;

        componentKafka.activate(null);
        componentKafka.register(mockPublisher);
        componentKafka.unregister(mockPublisher);
    }

}
