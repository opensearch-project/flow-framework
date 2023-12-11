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
 * External Action for public facing RestGetWorkflowAction
 */
public class GetWorkflowAction extends ActionType<GetWorkflowResponse> {
    /** The name of this action */
    public static final String NAME = TRANSPORT_ACTION_NAME_PREFIX + "workflow/get";
    /** An instance of this action */
    public static final GetWorkflowAction INSTANCE = new GetWorkflowAction();

    private GetWorkflowAction() {
        super(NAME, GetWorkflowResponse::new);
    }
}
