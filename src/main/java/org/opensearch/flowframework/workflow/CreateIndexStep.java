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
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.index.mapper.MapperService;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;
import static org.opensearch.flowframework.exception.WorkflowStepException.getSafeException;

/**
 * Step to create an index
 */
public class CreateIndexStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateIndexStep.class);
    private final Client client;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "create_index";

    /**
     * Instantiate this class
     *
     * @param client Client to create an index
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public CreateIndexStep(Client client, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
        this.client = client;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        PlainActionFuture<WorkflowData> createIndexFuture = PlainActionFuture.newFuture();

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
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

            try {
                if (!configurations.isEmpty()) {
                    Map<String, Object> sourceAsMap = XContentHelper.convertToMap(configurationsBytes, false, MediaTypeRegistry.JSON).v2();
                    sourceAsMap = prepareMappings(sourceAsMap);
                    createIndexRequest.source(sourceAsMap, LoggingDeprecationHandler.INSTANCE);
                }
            } catch (Exception ex) {
                String errorMessage = "Failed to create the index" + indexName + ", _doc is not permitted in mapping";
                logger.error(errorMessage, ex);
                createIndexFuture.onFailure(new WorkflowStepException(errorMessage, RestStatus.BAD_REQUEST));
            }

            client.admin().indices().create(createIndexRequest, ActionListener.wrap(acknowledgedResponse -> {
                logger.info("Created index: {}", indexName);
                flowFrameworkIndicesHandler.addResourceToStateIndex(
                    currentNodeInputs,
                    currentNodeId,
                    getName(),
                    indexName,
                    createIndexFuture
                );
            }, ex -> {
                Exception e = getSafeException(ex);
                String errorMessage = (e == null ? "Failed to create the index " + indexName : e.getMessage());
                logger.error(errorMessage, e);
                createIndexFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
            }));
        } catch (Exception e) {
            createIndexFuture.onFailure(e);
        }

        return createIndexFuture;
    }

    // This method to check if the mapping contains a type `_doc` and if yes we fail the request
    // is to duplicate the behavior we have today through create index rest API, we want users
    // to encounter the same behavior and not suddenly have to add `_doc` while using our create_index step
    // related bug: https://github.com/opensearch-project/OpenSearch/issues/12775
    private static Map<String, Object> prepareMappings(Map<String, Object> source) {
        if (!source.containsKey("mappings") || !(source.get("mappings") instanceof Map)) {
            return source;
        }

        Map<String, Object> newSource = new HashMap<>(source);

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) source.get("mappings");
        if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, mappings)) {
            throw new WorkflowStepException("The mapping definition cannot be nested under a type", RestStatus.BAD_REQUEST);
        }

        newSource.put("mappings", singletonMap(MapperService.SINGLE_MAPPING_NAME, mappings));
        return newSource;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
