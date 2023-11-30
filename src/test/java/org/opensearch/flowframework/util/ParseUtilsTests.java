/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class ParseUtilsTests extends OpenSearchTestCase {
    public void testToInstant() throws IOException {
        long epochMilli = Instant.now().toEpochMilli();
        XContentBuilder builder = XContentFactory.jsonBuilder().value(epochMilli);
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        Instant instant = ParseUtils.parseInstant(parser);
        assertEquals(epochMilli, instant.toEpochMilli());
    }

    public void testToInstantWithNullToken() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value((Long) null);
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        XContentParser.Token token = parser.currentToken();
        assertEquals(token, XContentParser.Token.VALUE_NULL);
        Instant instant = ParseUtils.parseInstant(parser);
        assertNull(instant);
    }

    public void testToInstantWithNullValue() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value(randomLong());
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        parser.nextToken();
        XContentParser.Token token = parser.currentToken();
        assertNull(token);
        Instant instant = ParseUtils.parseInstant(parser);
        assertNull(instant);
    }

    public void testToInstantWithNotValue() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().nullField("test").endObject();
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        Instant instant = ParseUtils.parseInstant(parser);
        assertNull(instant);
    }

    public void testGetParameterMap() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("key1", "value1");
        parameters.put("key2", 2);
        parameters.put("key3", 2.1);
        parameters.put("key4", new int[] { 10, 20 });
        parameters.put("key5", new Object[] { 1.01, "abc" });
        Map<String, String> parameterMap = ParseUtils.getParameterMap(parameters);
        Assert.assertEquals(5, parameterMap.size());
        Assert.assertEquals("value1", parameterMap.get("key1"));
        Assert.assertEquals("2", parameterMap.get("key2"));
        Assert.assertEquals("2.1", parameterMap.get("key3"));
        Assert.assertEquals("[10,20]", parameterMap.get("key4"));
        Assert.assertEquals("[1.01,\"abc\"]", parameterMap.get("key5"));
    }
}
