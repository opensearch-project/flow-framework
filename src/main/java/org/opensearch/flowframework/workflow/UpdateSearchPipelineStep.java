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
import org.opensearch.action.search.PutSearchPipelineRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;

/**
 * Step to update a search pipeline
 */
public class UpdateSearchPipelineStep extends AbstractUpdatePipelineStep {
    private static final Logger logger = LogManager.getLogger(UpdateSearchPipelineStep.class);

    /** The name of this step, used as a key in the {@link WorkflowStepFactory} */
    public static final String NAME = "update_search_pipeline";

    /**
     * Instantiates a new UpdateSearchPipelineStep
     * @param client The client to create a pipeline and store workflow data into the global context index
     */
    public UpdateSearchPipelineStep(Client client) {
        super(client);
    }

    @Override
    public void executePutPipelineRequest(
        String pipelineId,
        BytesReference configuration,
        ClusterAdminClient clusterAdminClient,
        ActionListener<AcknowledgedResponse> listener
    ) {
        PutSearchPipelineRequest putSearchPipelineRequest = new PutSearchPipelineRequest(pipelineId, configuration, XContentType.JSON);
        clusterAdminClient.putSearchPipeline(putSearchPipelineRequest, listener);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
