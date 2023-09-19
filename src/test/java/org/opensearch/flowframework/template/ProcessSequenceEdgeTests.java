/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.test.OpenSearchTestCase;

public class ProcessSequenceEdgeTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testEdge() {
        ProcessSequenceEdge edgeAB = new ProcessSequenceEdge("A", "B");
        assertEquals("A", edgeAB.getSource());
        assertEquals("B", edgeAB.getDestination());
        assertEquals("A->B", edgeAB.toString());

        ProcessSequenceEdge edgeAB2 = new ProcessSequenceEdge("A", "B");
        assertEquals(edgeAB, edgeAB2);

        ProcessSequenceEdge edgeAC = new ProcessSequenceEdge("A", "C");
        assertNotEquals(edgeAB, edgeAC);
    }
}
