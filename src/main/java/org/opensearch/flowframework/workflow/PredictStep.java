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
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.MLInputDataset;
import org.opensearch.ml.common.dataset.SearchQueryInputDataset;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.MLOutput;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.INCLUDES;
import static org.opensearch.flowframework.common.CommonValue.INPUT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.VECTORS;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;

/**
 * Step to predict data
 */
public class PredictStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(PredictStep.class);

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "predict";
    private MachineLearningNodeClient mlClient;

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     */
    public PredictStep(MachineLearningNodeClient mlClient) {
        this.mlClient = mlClient;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        PlainActionFuture<WorkflowData> predictFuture = PlainActionFuture.newFuture();

        ActionListener<MLOutput> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(MLOutput mlOutput) {
                logger.info("Prediction done. Storing vectors");
                final ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlOutput;
                final List<ModelTensors> vectors = modelTensorOutput.getMlModelOutputs();

                predictFuture.onResponse(
                    new WorkflowData(Map.ofEntries(Map.entry(VECTORS, vectors)), currentNodeInputs.getWorkflowId(), currentNodeId)
                );
            }

            @Override
            public void onFailure(Exception e) {
                String errorMessage = "Failed to predict the data";
                logger.error(errorMessage, e);
                predictFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
            }
        };

        Set<String> requiredKeys = Set.of(MODEL_ID, INPUT_INDEX, INCLUDES);

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

            String modelId = (String) inputs.get(MODEL_ID);
            List<String> indexes = (List<String>) inputs.get(INPUT_INDEX);
            String[] includes = (String[]) inputs.get(INCLUDES);

            MLInputDataset inputDataset = new SearchQueryInputDataset(indexes, generateQuery(includes));

            MLInput mlInput = new MLInput(FunctionName.KMEANS, null, inputDataset);

            mlClient.predict(modelId, mlInput, actionListener);

        } catch (Exception e) {
            predictFuture.onFailure(e);
        }
        return predictFuture;
    }

    private SearchSourceBuilder generateQuery(String[] includes) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(1000);
        searchSourceBuilder.fetchSource(includes, null);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        return searchSourceBuilder;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
