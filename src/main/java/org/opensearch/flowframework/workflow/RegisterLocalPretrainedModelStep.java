/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;
import org.opensearch.threadpool.ThreadPool;

import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.DEPLOY_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.MODEL_FORMAT;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.VERSION_FIELD;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_GROUP_ID;

/**
 * Step to register an OpenSearch provided pretrained local model
 */
public class RegisterLocalPretrainedModelStep extends AbstractRegisterLocalModelStep {
    private final MachineLearningNodeClient mlClient;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "register_local_pretrained_model";

    /**
     * Instantiate this class
     * @param threadPool The OpenSearch thread pool
     * @param mlClient client to instantiate MLClient
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     * @param flowFrameworkSettings settings of flow framework
     */
    public RegisterLocalPretrainedModelStep(
        ThreadPool threadPool,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings
    ) {
        super(threadPool, mlClient, flowFrameworkIndicesHandler, flowFrameworkSettings);
        this.mlClient = mlClient;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    @Override
    protected Set<String> getRequiredKeys() {
        return Set.of(NAME_FIELD, VERSION_FIELD, MODEL_FORMAT);
    }

    @Override
    protected Set<String> getOptionalKeys() {
        return Set.of(DESCRIPTION_FIELD, MODEL_GROUP_ID, DEPLOY_FIELD);
    }

    @Override
    protected String getResourceId(MLTask response) {
        return response.getModelId();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
