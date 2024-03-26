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
import org.opensearch.action.search.SearchResponse;

import static org.opensearch.flowframework.common.CommonValue.TRANSPORT_ACTION_NAME_PREFIX;

/**
 * External action for public facing RestOrchestrateAction
 */
public class OrchestrateAction extends ActionType<SearchResponse> {

    /** The name of this action */
    public static final String NAME = TRANSPORT_ACTION_NAME_PREFIX + "workflow/orchestrate";

    /** An instance of this action */
    public static final OrchestrateAction INSTANCE = new OrchestrateAction();

    private OrchestrateAction() {
        super(NAME, SearchResponse::new);
    }
}
