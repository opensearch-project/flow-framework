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

/**
 * External Action for public facing RestProvisionWorkflowAction
 */
public class ProvisionWorkflowAction extends ActionType<WorkflowResponse> {

    // TODO : Determine external action prefix for plugin
    /** The name of this action */
    public static final String NAME = "workflows/provision";
    /** An instance of this action */
    public static final ProvisionWorkflowAction INSTANCE = new ProvisionWorkflowAction();

    private ProvisionWorkflowAction() {
        super(NAME, WorkflowResponse::new);
    }
}
