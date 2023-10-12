/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.TemplateUtil.buildStringToStringMap;
import static org.opensearch.flowframework.common.TemplateUtil.parseStringToStringMap;

/**
 * This represents a processor associated with search and ingest pipelines in the {@link Template}.
 */
public class PipelineProcessor implements ToXContentObject {

    /** The type field name for pipeline processors */
    public static final String TYPE_FIELD = "type";
    /** The params field name for pipeline processors */
    public static final String PARAMS_FIELD = "params";

    private final String type;
    private final Map<String, String> params;

    /**
     * Create this processor with a type and map of parameters
     * @param type the processor type
     * @param params a map of params
     */
    public PipelineProcessor(String type, Map<String, String> params) {
        this.type = type;
        this.params = params;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field(TYPE_FIELD, this.type);
        xContentBuilder.field(PARAMS_FIELD);
        buildStringToStringMap(xContentBuilder, this.params);
        return xContentBuilder.endObject();
    }

    /**
     * Parse raw json content into a processor instance.
     *
     * @param parser json based content parser
     * @return the parsed PipelineProcessor instance
     * @throws IOException if content can't be parsed correctly
     */
    public static PipelineProcessor parse(XContentParser parser) throws IOException {
        String type = null;
        Map<String, String> params = new HashMap<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case TYPE_FIELD:
                    type = parser.text();
                    break;
                case PARAMS_FIELD:
                    params = parseStringToStringMap(parser);
                    break;
                default:
                    throw new IOException("Unable to parse field [" + fieldName + "] in a pipeline processor object.");
            }
        }
        if (type == null) {
            throw new IOException("A processor object requires a type field.");
        }

        return new PipelineProcessor(type, params);
    }

    /**
     * Get the processor type
     * @return the type
     */
    public String type() {
        return type;
    }

    /**
     * Get the processor params
     * @return the params
     */
    public Map<String, String> params() {
        return params;
    }
}
