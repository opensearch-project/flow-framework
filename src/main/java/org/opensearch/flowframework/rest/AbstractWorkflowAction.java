/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BaseRestHandler;

import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOWS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.WORKFLOW_REQUEST_TIMEOUT;

/**
 * Abstract action for the rest actions
 */
public abstract class AbstractWorkflowAction extends BaseRestHandler {
    /** Timeout for the request*/
    protected volatile TimeValue requestTimeout;
    /** Max workflows that can be created*/
    protected volatile Integer maxWorkflows;

    /**
     * Instantiates a new AbstractWorkflowAction
     *
     * @param settings Environment settings
     * @param clusterService clusterService
     */
    public AbstractWorkflowAction(Settings settings, ClusterService clusterService) {
        this.requestTimeout = WORKFLOW_REQUEST_TIMEOUT.get(settings);
        this.maxWorkflows = MAX_WORKFLOWS.get(settings);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(WORKFLOW_REQUEST_TIMEOUT, it -> requestTimeout = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_WORKFLOWS, it -> maxWorkflows = it);
    }

}
