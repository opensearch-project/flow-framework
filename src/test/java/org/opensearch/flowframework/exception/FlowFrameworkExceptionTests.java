/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.exception;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class FlowFrameworkExceptionTests extends OpenSearchTestCase {

    public void testExceptions() {
        WorkflowStepException wse = new WorkflowStepException("WSE", RestStatus.OK);
        assertTrue(wse instanceof FlowFrameworkException);
        assertTrue(wse instanceof OpenSearchException);
        assertEquals(RestStatus.OK, ExceptionsHelper.status(wse));
        assertFalse(wse.isFragment());
    }

    public void testSerialize() throws IOException {
        FlowFrameworkException ffe = new FlowFrameworkException("FFE", RestStatus.OK);
        assertTrue(ffe instanceof OpenSearchException);
        assertEquals(RestStatus.OK, ExceptionsHelper.status(ffe));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            ffe.writeTo(out);
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                ffe = new FlowFrameworkException(in);
                assertTrue(ffe instanceof OpenSearchException);
                assertEquals(RestStatus.OK, ExceptionsHelper.status(ffe));
            }
        }

        XContentBuilder builder = JsonXContent.contentBuilder();
        assertEquals("{\"error\":\"FFE\"}", ffe.toXContent(builder, ToXContent.EMPTY_PARAMS).toString());
    }
}
