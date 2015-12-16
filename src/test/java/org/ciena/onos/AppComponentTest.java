/*
 * Copyright 2014 Open Networking Laboratory
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

import static org.easymock.EasyMock.isA;

import java.util.HashSet;
import java.util.Set;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onlab.osgi.ServiceDirectory;
import org.onlab.osgi.TestServiceDirectory;
import org.onlab.rest.BaseResource;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.cfg.ConfigProperty;
import org.onosproject.codec.CodecService;
import org.onosproject.codec.impl.CodecManager;
import org.onosproject.mastership.MastershipService;
import org.onosproject.net.DeviceId;
import org.onosproject.net.MastershipRole;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.link.LinkListener;
import org.onosproject.net.link.LinkService;

/**
 * Set of tests of the ONOS application component.
 */
public class AppComponentTest {

	private KafkaNotificationBridge component;

	protected DeviceService mockDeviceService;
	protected LinkService mockLinkService;
	protected ComponentConfigService mockConfigService;
	protected MastershipService mockMastershipService;

	@Before
	public void setUpMocks() {
		mockDeviceService = EasyMock.createMock(DeviceService.class);
		EasyMock.expect(mockDeviceService.isAvailable(isA(DeviceId.class))).andReturn(true).anyTimes();
		EasyMock.expect(mockDeviceService.getRole(isA(DeviceId.class))).andReturn(MastershipRole.MASTER).anyTimes();
		mockDeviceService.addListener(isA(DeviceListener.class));
		EasyMock.expectLastCall().once();
		mockDeviceService.removeListener(isA(DeviceListener.class));
		EasyMock.expectLastCall().once();

		mockLinkService = EasyMock.createMock(LinkService.class);
		mockLinkService.addListener(isA(LinkListener.class));
		EasyMock.expectLastCall().once();
		mockLinkService.removeListener(isA(LinkListener.class));
		EasyMock.expectLastCall().once();

		mockConfigService = EasyMock.createMock(ComponentConfigService.class);
		Set<ConfigProperty> config = new HashSet<ConfigProperty>();
		EasyMock.expect(mockConfigService.getProperties(isA(String.class))).andReturn(config).anyTimes();

		mockMastershipService = EasyMock.createMock(MastershipService.class);
		EasyMock.expect(mockMastershipService.isLocalMaster(isA(DeviceId.class))).andReturn(true).anyTimes();

		// Register the services needed for the test
		CodecManager codecService = new CodecManager();
		codecService.activate();
		ServiceDirectory testDirectory = new TestServiceDirectory().add(DeviceService.class, mockDeviceService)
				.add(CodecService.class, codecService);

		BaseResource.setServiceDirectory(testDirectory);

	}

	/**
	 * Verifies test mocks.
	 */
	@After
	public void tearDownMocks() {
		component.deactivate();
		EasyMock.verify(mockDeviceService);
	}

	@Test
	public void basics() {
		EasyMock.replay(mockDeviceService);
		component = new KafkaNotificationBridge();
		component.deviceService = mockDeviceService;
		component.linkService = mockLinkService;
		component.configService = mockConfigService;
		component.mastershipService = mockMastershipService;
		component.activate();
	}

}
