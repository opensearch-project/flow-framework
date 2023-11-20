/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;

import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_REQUEST_RETRY;

/**
 * Controls max request retry setting for workflow step transport action APIs
 */
public class FlowFrameworkMaxRequestRetrySetting {

    protected volatile Integer maxRetry;

    /**
     * Instantiate this class.
     *
     * @param clusterService OpenSearch cluster service
     * @param settings OpenSearch settings
     */
    public FlowFrameworkMaxRequestRetrySetting(ClusterService clusterService, Settings settings) {
        maxRetry = MAX_REQUEST_RETRY.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_REQUEST_RETRY, it -> maxRetry = it);
    }

    /**
    * Gets the maximum number of retries
    * @return the maximum number of retries
    */
    public int getMaxRetries() {
        return maxRetry;
    }
}
