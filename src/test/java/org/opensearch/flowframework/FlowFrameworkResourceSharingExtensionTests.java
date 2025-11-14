/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework;

import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.util.ResourceSharingClientAccessor;
import org.opensearch.security.spi.resources.ResourceProvider;
import org.opensearch.security.spi.resources.client.ResourceSharingClient;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

import static org.mockito.Mockito.mock;

public class FlowFrameworkResourceSharingExtensionTests extends OpenSearchTestCase {

    public void testGetResourceProviders() {
        FlowFrameworkResourceSharingExtension extension = new FlowFrameworkResourceSharingExtension();
        Set<ResourceProvider> providers = extension.getResourceProviders();

        assertEquals(2, providers.size());

        for (ResourceProvider provider : providers) {
            if (CommonValue.WORKFLOW_RESOURCE_TYPE.equals(provider.resourceType())) {
                assertEquals(CommonValue.GLOBAL_CONTEXT_INDEX, provider.resourceIndexName());
            } else {
                assertEquals(CommonValue.WORKFLOW_STATE_RESOURCE_TYPE, provider.resourceType());
                assertEquals(CommonValue.WORKFLOW_STATE_INDEX, provider.resourceIndexName());
            }
        }
    }

    public void testAssignResourceSharingClient() {
        FlowFrameworkResourceSharingExtension extension = new FlowFrameworkResourceSharingExtension();
        ResourceSharingClient mockClient = mock(ResourceSharingClient.class);

        extension.assignResourceSharingClient(mockClient);

        assertEquals(mockClient, ResourceSharingClientAccessor.getInstance().getResourceSharingClient());
    }
}
