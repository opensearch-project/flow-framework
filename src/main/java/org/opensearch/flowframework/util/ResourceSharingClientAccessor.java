/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.security.spi.resources.client.ResourceSharingClient;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Accessor for resource sharing client
 */
public class ResourceSharingClientAccessor {
    private final AtomicReference<ResourceSharingClient> client = new AtomicReference<>();

    private static final ResourceSharingClientAccessor INSTANCE = new ResourceSharingClientAccessor();

    private ResourceSharingClientAccessor() {}

    /**
     * Get the singleton instance of ResourceSharingClientAccessor
     * @return the singleton instance
     */
    public static ResourceSharingClientAccessor getInstance() {
        return INSTANCE;
    }

    /**
     * Set the resource sharing client
     * @param client the resource sharing client to set
     */
    public void setResourceSharingClient(ResourceSharingClient client) {
        this.client.set(client);
    }

    /**
     * Get the resource sharing client
     * @return the resource sharing client
     */
    public ResourceSharingClient getResourceSharingClient() {
        return this.client.get();
    }
}
