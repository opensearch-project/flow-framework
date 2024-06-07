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
import org.opensearch.common.Booleans;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.ReindexAction;
import org.opensearch.index.reindex.ReindexRequest;

import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.DESTINATION_INDEX;
import static org.opensearch.flowframework.common.CommonValue.SOURCE_INDEX;

/**
 * Step to reindex
 */
public class ReindexStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(ReindexStep.class);
    private final Client client;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "reindex";
    /** The refresh field for reindex */
    private static final String REFRESH = "refresh";
    /** The requests_per_second field for reindex */
    private static final String REQUESTS_PER_SECOND = "requests_per_second";
    /** The require_alias field for reindex */
    private static final String REQUIRE_ALIAS = "require_alias";
    /** The slices field for reindex */
    private static final String SLICES = "slices";
    /** The max_docs field for reindex */
    private static final String MAX_DOCS = "max_docs";

    /**
     * Instantiate this class
     *
     * @param client Client to create an index
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public ReindexStep(Client client, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
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

        Set<String> optionalKeys = Set.of(REFRESH, REQUESTS_PER_SECOND, REQUIRE_ALIAS, SLICES, MAX_DOCS);

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
            Boolean refresh = inputs.containsKey(REFRESH) ? Booleans.parseBoolean(inputs.get(REFRESH).toString()) : null;
            Float requestsPerSecond = inputs.containsKey(REQUESTS_PER_SECOND)
                ? Float.parseFloat(inputs.get(REQUESTS_PER_SECOND).toString())
                : null;
            requestsPerSecond = requestsPerSecond < 0 ? Float.POSITIVE_INFINITY : requestsPerSecond;
            Boolean requireAlias = inputs.containsKey(REQUIRE_ALIAS) ? Booleans.parseBoolean(inputs.get(REQUIRE_ALIAS).toString()) : null;
            Integer slices;
            Integer maxDocs;
            if (inputs.get(SLICES) != null) {
                slices = Integer.parseInt(String.valueOf(inputs.get(SLICES)));
            } else {
                slices = (Integer) inputs.get(SLICES);
            }
            if (inputs.get(MAX_DOCS) != null) {
                maxDocs = Integer.parseInt(String.valueOf(inputs.get(MAX_DOCS)));
            } else {
                maxDocs = (Integer) inputs.get(MAX_DOCS);
            }
            ReindexRequest reindexRequest = new ReindexRequest().setSourceIndices(Strings.splitStringByCommaToArray(sourceIndices))
                .setDestIndex(destinationIndex);

            if (refresh != null) {
                reindexRequest.setRefresh(refresh);
            }
            if (requestsPerSecond != null) {
                reindexRequest.setRequestsPerSecond(requestsPerSecond);
            }
            if (requireAlias != null) {
                reindexRequest.setRequireAlias(requireAlias);
            }
            if (maxDocs != null) {
                reindexRequest.setMaxDocs(maxDocs);
            }
            if (slices != null) {
                reindexRequest.setSlices(slices);
            }

            ActionListener<BulkByScrollResponse> actionListener = new ActionListener<>() {

                @Override
                public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                    logger.info("Reindex from source: {} to destination {}", sourceIndices, destinationIndex);
                    try {
                        if (bulkByScrollResponse.getBulkFailures().isEmpty() && bulkByScrollResponse.getSearchFailures().isEmpty()) {
                            reIndexFuture.onResponse(
                                new WorkflowData(
                                    Map.of(
                                        NAME,
                                        Map.ofEntries(
                                            Map.entry(DESTINATION_INDEX, destinationIndex),
                                            Map.entry(SOURCE_INDEX, sourceIndices)
                                        )
                                    ),
                                    currentNodeInputs.getWorkflowId(),
                                    currentNodeInputs.getNodeId()
                                )
                            );
                        } else {
                            String errorMessage = "Failed to get bulk response " + bulkByScrollResponse.getBulkFailures();
                            reIndexFuture.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                        }
                    } catch (Exception e) {
                        String errorMessage = "Failed to parse and update new created resource";
                        logger.error(errorMessage, e);
                        reIndexFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    String errorMessage = "Failed to reindex from source " + sourceIndices + " to " + destinationIndex;
                    logger.error(errorMessage, e);
                    reIndexFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
                }
            };

            client.execute(ReindexAction.INSTANCE, reindexRequest, actionListener);

        } catch (IllegalArgumentException iae) {
            String error = "Failed to reindex " + iae.getMessage();
            reIndexFuture.onFailure(new WorkflowStepException(error, RestStatus.BAD_REQUEST));
        } catch (Exception e) {
            reIndexFuture.onFailure(e);
        }

        return reIndexFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
