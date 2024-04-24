/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.mockito.Mockito.mock;

public class ConfigTests extends OpenSearchTestCase {
    private NamedXContentRegistry xContentRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.xContentRegistry = mock(NamedXContentRegistry.class);
    }

    public void testConfig() throws IOException {
        String masterKey = "foo";
        Instant createTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Config config = new Config(masterKey, createTime);

        assertEquals(masterKey, config.masterKey());
        assertEquals(createTime, config.createTime());

        BytesReference bytesRef;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = config.toXContent(builder, ToXContent.EMPTY_PARAMS);
            bytesRef = BytesReference.bytes(source);
        }
        try (XContentParser parser = ParseUtils.createXContentParserFromRegistry(xContentRegistry, bytesRef)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            config = Config.parse(parser);
        }
        assertEquals(masterKey, config.masterKey());
        assertEquals(createTime, config.createTime());
    }
}
