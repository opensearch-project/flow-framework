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

import static org.opensearch.flowframework.common.CommonValue.TRANSPORT_ACION_NAME_PREFIX;

public class SearchWorkflowAction extends ActionType<SearchResponse> {

    /** The name of this action */
    public static final String NAME = TRANSPORT_ACION_NAME_PREFIX + "workflow/search";
    /** An instance of this action */
    public static final SearchWorkflowAction INSTANCE = new SearchWorkflowAction();

    private SearchWorkflowAction() {
        super(NAME, SearchResponse::new);
    }
}
