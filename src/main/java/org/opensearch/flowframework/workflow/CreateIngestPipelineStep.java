/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.client.Client;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;

/**
 * Step to create an ingest pipeline
 */
public class CreateIngestPipelineStep extends AbstractCreatePipelineStep {
    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "create_ingest_pipeline";

    /**
     * Instantiates a new CreateIngestPipelineStep
     * @param client The client to create a pipeline and store workflow data into the global context index
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public CreateIngestPipelineStep(Client client, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
        super(client, flowFrameworkIndicesHandler);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
