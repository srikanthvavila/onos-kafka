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
package org.ciena.onos.ext_notifier.api;

import java.util.Set;

/**
 * Registry for tracking event publishers.
 *
 * @param
 *            <P>
 *            type of the event publisher
 */
public interface PublisherRegistry<P extends PublisherSource> {
    /**
     * Registers the supplied event publisher with the Registry.
     *
     * @param publisherSource
     *            event publisher
     * @throws java.lang.IllegalArgumentException
     *             if the provider is registered already
     */
    void register(P publisherSource);

    /**
     * Unregisters the supplied event publisher.
     * <p>
     * Unregistering a publisher that has not been previously registered results
     * in a no-op.
     * </p>
     *
     * @param publisherSource
     *            event publisher
     */
    void unregister(P publisherSource);

    /**
     * Returns a set of currently registered event publishers.
     *
     * @return set of event publisher objects
     */
    Set<PublisherSource> getPublihserSources();

    /**
     * Returns a registered event publishers for the given "publishes"
     * identifier.
     *
     * @return event publisher object if found else null
     */
    PublisherSource getPublisherSource(String publishes);
}
