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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.search.pipeline.PipelineConfiguration;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.Processor.PipelineContext;
import org.opensearch.search.pipeline.Processor.PipelineSource;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROCESSOR_CONFIG;
import static org.opensearch.flowframework.common.CommonValue.SEARCH_REQUEST;
import static org.opensearch.flowframework.common.CommonValue.SEARCH_RESPONSE;
import static org.opensearch.flowframework.common.CommonValue.TAG;
import static org.opensearch.flowframework.common.CommonValue.TYPE;

/**
 * Step to create a search response processor
 */
public class SearchResponseProcessorStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(SearchResponseProcessorStep.class);
    private final SearchPipelineService searchPipelineService;
    /**
     * The name of the SearchResponseProcessor step
     */
    public static final String NAME = "search_response_processor";

    /**
     * Creates a new SearchResponseProcessorStep
     * @param searchPipelineService the search pipeline service
     */
    public SearchResponseProcessorStep(SearchPipelineService searchPipelineService) {
        this.searchPipelineService = searchPipelineService;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        PlainActionFuture<WorkflowData> searchResponseProcessorFuture = PlainActionFuture.newFuture();

        Set<String> requiredKeys = Set.of(TYPE, PROCESSOR_CONFIG, TAG, DESCRIPTION_FIELD, SEARCH_REQUEST, SEARCH_RESPONSE);
        Set<String> optionalKeys = Collections.emptySet();

        Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
            requiredKeys,
            optionalKeys,
            currentNodeInputs,
            outputs,
            previousNodeInputs,
            params
        );
        String type = (String) inputs.get(TYPE);
        PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(
            "id",
            new BytesArray((String) inputs.get(PROCESSOR_CONFIG)),
            MediaTypeRegistry.JSON
        );
        String tag = (String) inputs.get(TAG);
        String description = (String) inputs.get(DESCRIPTION_FIELD);
        SearchRequest searchRequest = (SearchRequest) inputs.get(SEARCH_REQUEST);
        SearchResponse searchResponse = (SearchResponse) inputs.get(SEARCH_RESPONSE);

        try {
            // Retrieve response processor factories
            Map<String, Processor.Factory<SearchResponseProcessor>> responseProcessors = searchPipelineService
                .getResponseProcessorFactories();

            // Create an instance of the processor
            SearchResponseProcessor processor = responseProcessors.get(type)
                .create(
                    responseProcessors,
                    tag,
                    description,
                    false,
                    pipelineConfiguration.getConfigAsMap(),
                    new PipelineContext(PipelineSource.UPDATE_PIPELINE)
                );

            // Temp : rerank processor only invokes processResponse async, doesnt need request context
            processor.processResponseAsync(searchRequest, searchResponse, null, ActionListener.wrap(processedSearchResponse -> {
                searchResponseProcessorFuture.onResponse(
                    new WorkflowData(
                        Map.ofEntries(Map.entry(SEARCH_RESPONSE, processedSearchResponse)),
                        currentNodeInputs.getWorkflowId(),
                        currentNodeInputs.getNodeId()
                    )
                );
            }, exception -> { searchResponseProcessorFuture.onFailure(exception); }));

        } catch (Exception e) {
            searchResponseProcessorFuture.onFailure(e);
        }
        return searchResponseProcessorFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
