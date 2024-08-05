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
 * External Action for public facing RestCreateWorkflowAction
 */
public class ReprovisionWorkflowAction extends ActionType<WorkflowResponse> {

    /** The name of this action */
    public static final String NAME = TRANSPORT_ACTION_NAME_PREFIX + "workflow/reprovision";
    /** An instance of this action */
    public static final ReprovisionWorkflowAction INSTANCE = new ReprovisionWorkflowAction();

    private ReprovisionWorkflowAction() {
        super(NAME, WorkflowResponse::new);
    }
}
