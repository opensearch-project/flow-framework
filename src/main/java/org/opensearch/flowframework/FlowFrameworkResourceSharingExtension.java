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

/**
 * Implementation for sharing resources that require access control.
 */
public class FlowFrameworkResourceSharingExtension implements ResourceSharingExtension {

    /** Instantiate this class */
    public FlowFrameworkResourceSharingExtension() {}

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

            // Workflow state documents are companion resources of the workflow
            // (template) they track: access is inherited from the parent
            // workflow's sharing record. This also covers state documents
            // written without an authenticated user in the thread context
            // (e.g. during provisioning steps executed under system context).
            @Override
            public String parentType() {
                return CommonValue.WORKFLOW_RESOURCE_TYPE;
            }

            @Override
            public String parentIdField() {
                return CommonValue.WORKFLOW_ID_FIELD;
            }
        });
    }

    @Override
    public void assignResourceSharingClient(ResourceSharingClient resourceSharingClient) {
        ResourceSharingClientAccessor.getInstance().setResourceSharingClient(resourceSharingClient);
    }
}
