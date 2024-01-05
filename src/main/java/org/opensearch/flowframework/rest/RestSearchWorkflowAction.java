/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.transport.SearchWorkflowAction;

import java.util.List;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;

/**
 * Rest Action to facilitate requests to search workflows
 */
public class RestSearchWorkflowAction extends AbstractSearchWorkflowAction<Template> {

    private static final String SEARCH_WORKFLOW_ACTION = "search_workflow_action";
    private static final String SEARCH_WORKFLOW_PATH = WORKFLOW_URI + "/_search";

    /**
     * Instantiates a new RestSearchWorkflowAction
     *
     * @param flowFrameworkFeatureEnabledSetting Whether this API is enabled
     */
    public RestSearchWorkflowAction(FlowFrameworkSettings flowFrameworkFeatureEnabledSetting) {
        super(
            List.of(SEARCH_WORKFLOW_PATH),
            GLOBAL_CONTEXT_INDEX,
            Template.class,
            SearchWorkflowAction.INSTANCE,
            flowFrameworkFeatureEnabledSetting
        );
    }

    @Override
    public String getName() {
        return SEARCH_WORKFLOW_ACTION;
    }
}
