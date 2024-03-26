/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

/**
 * Transport request for orchestrate API
 */
public class OrchestrateRequest extends ActionRequest {

    /**
     * The documentId of the workflow entry within the Global Context index
     */
    private String workflowId;

    /**
     * User inputs map
     */
    private Map<String, String> userInputs;

    /**
     * Creates a new orchestrate request
     * @param workflowId the workflow ID
     * @param userInputs the user inputs to substitute values for in the template
     */
    public OrchestrateRequest(String workflowId, Map<String, String> userInputs) {
        this.workflowId = workflowId;
        this.userInputs = userInputs;
    }

    /**
     * Creates a new orchestrate request from stream input
     * @param in the stream input
     * @throws IOException on error reading from the stream input
     */
    public OrchestrateRequest(StreamInput in) throws IOException {
        super(in);
        this.workflowId = in.readString();
        this.userInputs = in.readMap(StreamInput::readString, StreamInput::readString);
    }

    public String getWorkflowId() {
        return this.workflowId;
    }

    public Map<String, String> getUserInputs() {
        return this.userInputs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(workflowId);
        out.writeMap(userInputs, StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
