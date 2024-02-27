/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.mockito.ArgumentCaptor;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STEP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
public class GetWorkflowStepTransportActionTests extends OpenSearchTestCase {

    private GetWorkflowStepTransportAction getWorkflowStepTransportAction;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        this.getWorkflowStepTransportAction = new GetWorkflowStepTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(WorkflowStepFactory.class)
        );
    }

    public void testGetWorkflowStepAction() throws IOException {
        WorkflowRequest workflowRequest = new WorkflowRequest(null, null);
        ActionListener<GetWorkflowStepResponse> listener = mock(ActionListener.class);
        getWorkflowStepTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<GetWorkflowStepResponse> stepCaptor = ArgumentCaptor.forClass(GetWorkflowStepResponse.class);
        verify(listener, times(1)).onResponse(stepCaptor.capture());
    }

    public void testGetWorkflowStepValidator() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put(WORKFLOW_STEP, "create_connector, delete_model");

        WorkflowRequest workflowRequest = new WorkflowRequest(null, null, params);
        ActionListener<GetWorkflowStepResponse> listener = mock(ActionListener.class);
        getWorkflowStepTransportAction.doExecute(mock(Task.class), workflowRequest, listener);
        ArgumentCaptor<GetWorkflowStepResponse> stepCaptor = ArgumentCaptor.forClass(GetWorkflowStepResponse.class);
        verify(listener, times(1)).onResponse(stepCaptor.capture());
        assertEquals(GetWorkflowStepResponse.class, stepCaptor.getValue().getClass());

    }
}
