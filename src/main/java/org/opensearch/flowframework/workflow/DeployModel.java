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
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.transport.deploy.MLDeployModelResponse;

public class DeployModel {
    private static final Logger logger = LogManager.getLogger(DeployModel.class);

    public void deployModel(MachineLearningNodeClient machineLearningNodeClient, String modelId) {

        ActionListener<MLDeployModelResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLDeployModelResponse mlDeployModelResponse) {
                if (mlDeployModelResponse.getStatus() == MLTaskState.COMPLETED.name()) {
                    logger.info("Model deployed successfully");
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Model deployment failed");
            }
        };
        machineLearningNodeClient.deploy(modelId, actionListener);

    }
}
