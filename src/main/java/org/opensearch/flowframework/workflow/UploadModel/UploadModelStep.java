package org.opensearch.flowframework.workflow.UploadModel;

import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class UploadModelStep implements WorkflowStep {

    private final String NAME = "upload_model_step";

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {
        return null;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
