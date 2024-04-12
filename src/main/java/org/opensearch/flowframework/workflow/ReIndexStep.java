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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.ReindexAction;
import org.opensearch.index.reindex.ReindexRequest;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.DESTINATION_INDEX;
import static org.opensearch.flowframework.common.CommonValue.RE_INDEX_FIELD;
import static org.opensearch.flowframework.common.CommonValue.SOURCE_INDEX;

public class ReIndexStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(ReIndexStep.class);
    private final Client client;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "reindex";

    /**
     * Instantiate this class
     *
     * @param client Client to create an index
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public ReIndexStep(Client client, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
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

        PlainActionFuture<WorkflowData> reIndexFuture = PlainActionFuture.newFuture();

        Set<String> requiredKeys = Set.of(SOURCE_INDEX, DESTINATION_INDEX);

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

            String sourceIndices = (String) inputs.get(SOURCE_INDEX);
            String destinationIndex = (String) inputs.get(DESTINATION_INDEX);

            ReindexRequest reindexRequest = new ReindexRequest();
            reindexRequest.setSourceIndices(sourceIndices);
            reindexRequest.setDestIndex(destinationIndex);
            reindexRequest.setRefresh(true);

            ActionListener<BulkByScrollResponse> actionListener = new ActionListener<>() {

                @Override
                public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                    logger.info("Reindex from source: {} to destination {}", sourceIndices, destinationIndex);
                    try {
                        if (bulkByScrollResponse.getBulkFailures().isEmpty() && bulkByScrollResponse.getSearchFailures().isEmpty()) {
                            flowFrameworkIndicesHandler.updateResourceInStateIndex(
                                currentNodeInputs.getWorkflowId(),
                                currentNodeId,
                                getName(),
                                destinationIndex,
                                ActionListener.wrap(response -> {
                                    logger.info("successfully updated resource created in state index: {}", response.getIndex());

                                    reIndexFuture.onResponse(
                                        new WorkflowData(
                                            Map.of(RE_INDEX_FIELD, Map.of(sourceIndices, destinationIndex)),
                                            currentNodeInputs.getWorkflowId(),
                                            currentNodeInputs.getNodeId()
                                        )
                                    );
                                }, exception -> {
                                    String errorMessage = "Failed to update new reindexed"
                                        + currentNodeId
                                        + " resource "
                                        + getName()
                                        + " id "
                                        + destinationIndex;
                                    logger.error(errorMessage, exception);
                                    reIndexFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                                })
                            );
                        }
                    } catch (Exception e) {
                        String errorMessage = "Failed to parse and update new created resource";
                        logger.error(errorMessage, e);
                        reIndexFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    String errorMessage = "Failed to reindex from source" + sourceIndices + "to" + destinationIndex;
                    logger.error(errorMessage, e);
                    reIndexFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
                }
            };

            client.execute(ReindexAction.INSTANCE, reindexRequest, actionListener);

        } catch (Exception e) {
            reIndexFuture.onFailure(e);
        }

        return reIndexFuture;
    }

    @Override
    public String getName() {
        return null;
    }
}
