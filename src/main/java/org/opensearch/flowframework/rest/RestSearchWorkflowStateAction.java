/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import com.google.common.collect.ImmutableList;
import org.opensearch.flowframework.common.FlowFrameworkFeatureEnabledSetting;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.transport.SearchWorkflowStateAction;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;

/**
 * Rest Action to facilitate requests to search workflow states
 */
public class RestSearchWorkflowStateAction extends AbstractSearchWorkflowAction<WorkflowState> {

    private static final String SEARCH_WORKFLOW_STATE_ACTION = "search_workflow_state_action";
    private static final String SEARCH_WORKFLOW_STATE_PATH = WORKFLOW_URI + "/state/_search";

    /**
     * Instantiates a new RestSearchWorkflowStateAction
     *
     * @param flowFrameworkFeatureEnabledSetting Whether this API is enabled
     */
    public RestSearchWorkflowStateAction(FlowFrameworkFeatureEnabledSetting flowFrameworkFeatureEnabledSetting) {
        super(
            ImmutableList.of(SEARCH_WORKFLOW_STATE_PATH),
            WORKFLOW_STATE_INDEX,
            WorkflowState.class,
            SearchWorkflowStateAction.INSTANCE,
            flowFrameworkFeatureEnabledSetting
        );
    }

    @Override
    public String getName() {
        return SEARCH_WORKFLOW_STATE_ACTION;
    }

}
