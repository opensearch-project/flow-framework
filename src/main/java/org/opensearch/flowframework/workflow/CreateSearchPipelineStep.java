/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.transport.client.Client;

/**
 * Step to create a search pipeline
 */
public class CreateSearchPipelineStep extends AbstractCreatePipelineStep {
    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "create_search_pipeline";

    /**
     * Instantiates a new CreateSearchPipelineStep
     * @param client The client to create a pipeline and store workflow data into the global context index
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public CreateSearchPipelineStep(Client client, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
        super(client, flowFrameworkIndicesHandler);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
