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
 * External Action for public facing RestSearchWorkflowStateAction
 */
public class SearchWorkflowStateAction extends ActionType<SearchResponse> {

    /** The name of this action */
    public static final String NAME = TRANSPORT_ACTION_NAME_PREFIX + "workflow_state/search";
    /** An instance of this action */
    public static final SearchWorkflowStateAction INSTANCE = new SearchWorkflowStateAction();

    private SearchWorkflowStateAction() {
        super(NAME, SearchResponse::new);
    }
}
