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
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;

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

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).mapping(configurationsBytes, XContentType.JSON);
            client.admin().indices().create(createIndexRequest, ActionListener.wrap(acknowledgedResponse -> {
                String resourceName = getResourceByWorkflowStep(getName());
                logger.info("Created index: {}", indexName);
                try {
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        currentNodeInputs.getWorkflowId(),
                        currentNodeId,
                        getName(),
                        indexName,
                        ActionListener.wrap(response -> {
                            logger.info("successfully updated resource created in state index: {}", response.getIndex());
                            createIndexFuture.onResponse(
                                new WorkflowData(Map.of(resourceName, indexName), currentNodeInputs.getWorkflowId(), currentNodeId)
                            );
                        }, exception -> {
                            String errorMessage = "Failed to update new created "
                                + currentNodeId
                                + " resource "
                                + getName()
                                + " id "
                                + indexName;
                            logger.error(errorMessage, exception);
                            createIndexFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                        })
                    );
                } catch (IOException ex) {
                    String errorMessage = "Failed to parse and update new created resource";
                    logger.error(errorMessage, ex);
                    createIndexFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(ex)));
                }
            }, e -> {
                String errorMessage = "Failed to create index";
                logger.error(errorMessage, e);
                createIndexFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }));
        } catch (Exception e) {
            createIndexFuture.onFailure(e);
        }

        return createIndexFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
