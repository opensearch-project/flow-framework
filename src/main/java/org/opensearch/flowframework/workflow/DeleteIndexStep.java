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
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.util.ParseUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;
import static org.opensearch.flowframework.exception.WorkflowStepException.getSafeException;

/**
 * Step to delete an index
 */
public class DeleteIndexStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(DeleteIndexStep.class);
    private final Client client;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "delete_index";
    /** Required input keys */
    public static final Set<String> REQUIRED_INPUTS = Set.of(INDEX_NAME);
    /** Optional input keys */
    public static final Set<String> OPTIONAL_INPUTS = Collections.emptySet();
    /** Provided output keys */
    public static final Set<String> PROVIDED_OUTPUTS = Set.of(INDEX_NAME);

    /**
     * Instantiate this class
     *
     * @param client Client to delete an index
     */
    public DeleteIndexStep(Client client) {
        this.client = client;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params,
        String tenantId
    ) {
        PlainActionFuture<WorkflowData> deleteIndexFuture = PlainActionFuture.newFuture();

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                REQUIRED_INPUTS,
                OPTIONAL_INPUTS,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            String indexName = (String) inputs.get(INDEX_NAME);

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);

            client.admin().indices().delete(deleteIndexRequest, ActionListener.wrap(acknowledgedResponse -> {
                logger.info("Deleted index: {}", indexName);
                deleteIndexFuture.onResponse(
                    new WorkflowData(
                        Map.ofEntries(Map.entry(INDEX_NAME, indexName)),
                        currentNodeInputs.getWorkflowId(),
                        currentNodeInputs.getNodeId()
                    )
                );
            }, ex -> {
                Exception e = getSafeException(ex);
                String errorMessage = (e == null
                    ? ParameterizedMessageFactory.INSTANCE.newMessage("Failed to delete the index {}", indexName).getFormattedMessage()
                    : e.getMessage());
                logger.error(errorMessage, e);
                deleteIndexFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
            }));
        } catch (Exception e) {
            deleteIndexFuture.onFailure(e);
        }
        return deleteIndexFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean allowDeleteRequired() {
        return true;
    }
}
