/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.exception;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class WorkflowStepExceptionTests extends OpenSearchTestCase {

    public void testBasicConstructors() {
        // Test message constructor
        WorkflowStepException exception1 = new WorkflowStepException("test message", RestStatus.BAD_REQUEST);
        assertEquals("test message", exception1.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, exception1.getRestStatus());

        // Test cause constructor
        Exception cause = new Exception("cause");
        WorkflowStepException exception2 = new WorkflowStepException(cause, RestStatus.NOT_FOUND);
        assertEquals(cause, exception2.getCause());
        assertEquals(RestStatus.NOT_FOUND, exception2.getRestStatus());

        // Test message and cause constructor
        WorkflowStepException exception3 = new WorkflowStepException("test", cause, RestStatus.OK);
        assertEquals("test", exception3.getMessage());
        assertEquals(cause, exception3.getCause());
        assertEquals(RestStatus.OK, exception3.getRestStatus());
    }

    public void testToXContent() throws IOException {
        WorkflowStepException exception = new WorkflowStepException("test message", RestStatus.BAD_REQUEST);
        XContentBuilder builder = JsonXContent.contentBuilder();
        exception.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"error\":\"test message\"}", builder.toString());
    }

    public void testGetSafeExceptionWithDirectExceptions() {
        // Test IllegalArgumentException
        Exception illegalArg = new IllegalArgumentException("illegal");
        assertEquals(illegalArg, WorkflowStepException.getSafeException(illegalArg));

        // Test OpenSearchStatusException
        Exception statusException = new OpenSearchStatusException("status", RestStatus.BAD_REQUEST);
        assertEquals(statusException, WorkflowStepException.getSafeException(statusException));

        // Test OpenSearchParseException (has BAD_REQUEST status)
        Exception parseException = new OpenSearchParseException("parse");
        assertEquals(parseException, WorkflowStepException.getSafeException(parseException));

        // Test ResourceAlreadyExistsException (has BAD_REQUEST status)
        Exception resourceException = new ResourceAlreadyExistsException("resource");
        assertEquals(resourceException, WorkflowStepException.getSafeException(resourceException));
    }

    public void testGetSafeExceptionWithNestedExceptions() {
        // Test nested BAD_REQUEST exception
        OpenSearchStatusException cause = new OpenSearchStatusException("cause", RestStatus.BAD_REQUEST);
        OpenSearchException wrapper = new OpenSearchException("wrapper", cause);
        assertEquals(cause, WorkflowStepException.getSafeException(wrapper));

        // Test nested non-BAD_REQUEST exception
        OpenSearchStatusException nonBadRequestCause = new OpenSearchStatusException("cause", RestStatus.NOT_FOUND);
        OpenSearchException wrapper2 = new OpenSearchException("wrapper", nonBadRequestCause);
        assertNull(WorkflowStepException.getSafeException(wrapper2));
    }

    public void testGetSafeExceptionWithUnsafeExceptions() {
        // Test regular Exception
        assertNull(WorkflowStepException.getSafeException(new Exception("test")));

        // Test OpenSearchException with INTERNAL_SERVER_ERROR status
        assertNull(WorkflowStepException.getSafeException(new OpenSearchException("test")));

        // Test null cause
        Exception nullCauseException = new Exception("test");
        assertNull(WorkflowStepException.getSafeException(nullCauseException));
    }
}
