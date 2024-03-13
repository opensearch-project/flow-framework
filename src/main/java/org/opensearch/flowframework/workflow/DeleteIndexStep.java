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
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.util.ParseUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;

/**
 * Step to delete an index
 */
public class DeleteIndexStep implements WorkflowStep {
    private static final Logger logger = LogManager.getLogger(DeleteConnectorStep.class);

    private final Client client;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "delete_index";

    /**
     * Instantiate this class
     *
     * @param client Client to create an index
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
        Map<String, String> params
    ) {

        PlainActionFuture<WorkflowData> deleteIndexFuture = PlainActionFuture.newFuture();

        Set<String> requiredKeys = Set.of(INDEX_NAME);
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

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            client.admin().indices().delete(deleteIndexRequest, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    logger.info("Deleted index: {}", indexName);
                    deleteIndexFuture.onResponse(
                        new WorkflowData(
                            Map.ofEntries(Map.entry(INDEX_NAME, indexName)),
                            currentNodeInputs.getWorkflowId(),
                            currentNodeInputs.getNodeId()
                        )
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    String errorMessage = "Failed to delete the index " + indexName;
                    logger.error(errorMessage, e);
                    deleteIndexFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                }
            });
        } catch (FlowFrameworkException e) {
            deleteIndexFuture.onFailure(e);
        }

        return deleteIndexFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
