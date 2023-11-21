/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;

import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_REQUEST_RETRY;

/**
 * Abstract retryable workflow step
 */
public abstract class RetryableWorkflowStep implements WorkflowStep {

    /** The maximum number of transport request retries */
    protected volatile Integer maxRetry;

    /**
     * Instantiates a new Retryable workflow step
     * @param settings Environment settings
     * @param clusterService the cluster service
     */
    public RetryableWorkflowStep(Settings settings, ClusterService clusterService) {
        this.maxRetry = MAX_REQUEST_RETRY.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_REQUEST_RETRY, it -> maxRetry = it);
    }

}
