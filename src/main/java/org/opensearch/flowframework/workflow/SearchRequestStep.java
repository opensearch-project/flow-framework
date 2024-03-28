/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.CommonValue.SEARCH_REQUEST;
import static org.opensearch.flowframework.common.CommonValue.SEARCH_RESPONSE;
import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;

/**
 * Step for search request
 */
public class SearchRequestStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(SearchRequestStep.class);
    private final Client client;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "search_request";

    /**
     * Instantiate this class
     *
     * @param client Client to search on an index
     */
    public SearchRequestStep(Client client) {
        this.client = client;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        PlainActionFuture<WorkflowData> searchIndexFuture = PlainActionFuture.newFuture();

        Set<String> requiredKeys = Set.of(INDEX_NAME, CONFIGURATIONS);
        Set<String> optionalKeys = Collections.emptySet();

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            String indexName = (String) inputs.get(INDEX_NAME);

            String configurations = (String) inputs.get(CONFIGURATIONS);

            byte[] byteArr = configurations.getBytes(StandardCharsets.UTF_8);
            BytesReference configurationsBytes = new BytesArray(byteArr);
            SearchRequest searchRequest = new SearchRequest(indexName);

            try {
                if (!configurations.isEmpty()) {
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        configurationsBytes.streamInput()
                    );
                    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                    searchSourceBuilder.parseXContent(parser);
                    searchRequest.source(searchSourceBuilder);
                }
            } catch (IOException ex) {
                String errorMessage = "Failed to search for the index based on the query;";
                logger.error(errorMessage, ex);
                searchIndexFuture.onFailure(new WorkflowStepException(errorMessage, RestStatus.BAD_REQUEST));
            }

            client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                searchIndexFuture.onResponse(
                    new WorkflowData(
                        Map.ofEntries(Map.entry(SEARCH_RESPONSE, searchResponse), Map.entry(SEARCH_REQUEST, searchRequest)),
                        currentNodeInputs.getWorkflowId(),
                        currentNodeId
                    )
                );
            }, exception -> {
                String errorMessage = "Failed to search on the index";
                logger.error(errorMessage, exception);
                searchIndexFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(exception)));
            }));

        } catch (Exception e) {
            searchIndexFuture.onFailure(e);
        }

        return searchIndexFuture;
    }

    @Override
    public String getName() {
        return null;
    }
}
