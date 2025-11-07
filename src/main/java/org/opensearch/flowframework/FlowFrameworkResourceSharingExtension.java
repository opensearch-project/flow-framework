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
import org.opensearch.security.spi.resources.ResourceSharingExtension;
import org.opensearch.security.spi.resources.client.ResourceSharingClient;

import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;

public class FlowFrameworkResourceSharingExtension implements ResourceSharingExtension {
    @Override
    public Set<ResourceProvider> getResourceProviders() {
        return Set.of(new ResourceProvider() {
            @Override
            public String resourceType() {
                return CommonValue.WORKFLOW_RESOURCE_TYPE;
            }

            @Override
            public String resourceIndexName() {
                return GLOBAL_CONTEXT_INDEX;
            }
        }, new ResourceProvider() {
            @Override
            public String resourceType() {
                return CommonValue.WORKFLOW_STATE_RESOURCE_TYPE;
            }

            @Override
            public String resourceIndexName() {
                return WORKFLOW_STATE_INDEX;
            }
        });
    }

    @Override
    public void assignResourceSharingClient(ResourceSharingClient resourceSharingClient) {
        ResourceSharingClientAccessor.getInstance().setResourceSharingClient(resourceSharingClient);
    }
}
