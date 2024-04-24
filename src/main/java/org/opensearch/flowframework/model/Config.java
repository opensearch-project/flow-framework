/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.util.ParseUtils;

import java.io.IOException;
import java.time.Instant;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.CREATE_TIME;
import static org.opensearch.flowframework.common.CommonValue.MASTER_KEY;

/**
 * Flow Framework Configuration
 */
public class Config implements ToXContentObject {

    private final String masterKey;
    private final Instant createTime;

    /**
     * Instantiate this object
     *
     * @param masterKey The encryption master key
     * @param createTime The config creation time
     */
    public Config(String masterKey, Instant createTime) {
        this.masterKey = masterKey;
        this.createTime = createTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field(MASTER_KEY, this.masterKey);
        xContentBuilder.field(CREATE_TIME, this.createTime.toEpochMilli());
        return xContentBuilder.endObject();
    }

    /**
     * Parse raw xContent into a Config instance.
     *
     * @param parser xContent based content parser
     * @return an instance of the config
     * @throws IOException if content can't be parsed correctly
     */
    public static Config parse(XContentParser parser) throws IOException {
        String masterKey = null;
        Instant createTime = Instant.now();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case MASTER_KEY:
                    masterKey = parser.text();
                    break;
                case CREATE_TIME:
                    createTime = ParseUtils.parseInstant(parser);
                    break;
                default:
                    throw new FlowFrameworkException(
                        "Unable to parse field [" + fieldName + "] in a config object.",
                        RestStatus.BAD_REQUEST
                    );
            }
        }
        if (masterKey == null) {
            throw new FlowFrameworkException("The config object requires a master key.", RestStatus.BAD_REQUEST);
        }
        return new Config(masterKey, createTime);
    }

    /**
     * @return the masterKey
     */
    public String masterKey() {
        return masterKey;
    }

    /**
     * @return the createTime
     */
    public Instant createTime() {
        return createTime;
    }
}
