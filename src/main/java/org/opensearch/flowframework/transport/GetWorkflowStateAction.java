/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.action.ActionType;

import static org.opensearch.flowframework.common.CommonValue.TRANSPORT_ACTION_NAME_PREFIX;

/**
 * External Action for public facing RestGetWorkflowStateAction
 */
public class GetWorkflowStateAction extends ActionType<GetWorkflowStateResponse> {
    // TODO : If the template body is returned as part of the GetWorkflowStateAction,
    // it is necessary to ensure the user has permissions for workflow/get
    /** The name of this action */
    public static final String NAME = TRANSPORT_ACTION_NAME_PREFIX + "workflow_state/get";
    /** An instance of this action */
    public static final GetWorkflowStateAction INSTANCE = new GetWorkflowStateAction();

    private GetWorkflowStateAction() {
        super(NAME, GetWorkflowStateResponse::new);
    }
}
