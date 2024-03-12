/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.CommonValue.TOOLS_ORDER_FIELD;
import static org.opensearch.flowframework.util.ParseUtils.buildStringToObjectMap;
import static org.opensearch.flowframework.util.ParseUtils.buildStringToStringMap;
import static org.opensearch.flowframework.util.ParseUtils.parseStringToObjectMap;
import static org.opensearch.flowframework.util.ParseUtils.parseStringToStringMap;

/**
 * This represents a process node (step) in a workflow graph in the {@link Template}.
 * It will have a one-to-one correspondence with a {@link ProcessNode},
 * where its type is used to determine the correct {@link WorkflowStep} object,
 * and its inputs are used to populate the {@link WorkflowData} input.
 */
public class WorkflowNode implements ToXContentObject {
    /** The template field name for node id */
    public static final String ID_FIELD = "id";
    /** The template field name for node type */
    public static final String TYPE_FIELD = "type";
    /** The template field name for previous node inputs */
    public static final String PREVIOUS_NODE_INPUTS_FIELD = "previous_node_inputs";
    /** The template field name for node inputs */
    public static final String USER_INPUTS_FIELD = "user_inputs";
    /** The field defining processors in the inputs for search and ingest pipelines */
    public static final String PROCESSORS_FIELD = "processors";
    /** The field defining the timeout value for this node */
    public static final String NODE_TIMEOUT_FIELD = "node_timeout";
    /** The default timeout value if the template doesn't override it */
    public static final TimeValue NODE_TIMEOUT_DEFAULT_VALUE = new TimeValue(10, SECONDS);

    private final String id; // unique id
    private final String type; // maps to a WorkflowStep
    private final Map<String, String> previousNodeInputs;
    private final Map<String, Object> userInputs; // maps to WorkflowData
    private static final Logger logger = LogManager.getLogger(WorkflowNode.class);

    /**
     * Create this node with the id and type, and any user input.
     *
     * @param id A unique string identifying this node
     * @param type The type of {@link WorkflowStep} to create for the corresponding {@link ProcessNode}
     * @param previousNodeInputs Optional input to identify inputs coming from predecessor nodes
     * @param userInputs Optional input to populate params in {@link WorkflowData}
     */
    public WorkflowNode(String id, String type, Map<String, String> previousNodeInputs, Map<String, Object> userInputs) {
        this.id = id;
        this.type = type;
        this.previousNodeInputs = Map.copyOf(previousNodeInputs);
        this.userInputs = Map.copyOf(userInputs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field(ID_FIELD, this.id);
        xContentBuilder.field(TYPE_FIELD, this.type);

        xContentBuilder.field(PREVIOUS_NODE_INPUTS_FIELD);
        buildStringToStringMap(xContentBuilder, previousNodeInputs);

        xContentBuilder.startObject(USER_INPUTS_FIELD);
        for (Entry<String, Object> e : userInputs.entrySet()) {
            xContentBuilder.field(e.getKey());
            if (e.getValue() instanceof String || e.getValue() instanceof Number || e.getValue() instanceof Boolean) {
                xContentBuilder.value(e.getValue());
            } else if (e.getValue() instanceof Map<?, ?>) {
                buildStringToStringMap(xContentBuilder, (Map<?, ?>) e.getValue());
            } else if (e.getValue() instanceof Object[]) {
                xContentBuilder.startArray();
                if (PROCESSORS_FIELD.equals(e.getKey())) {
                    for (PipelineProcessor p : (PipelineProcessor[]) e.getValue()) {
                        xContentBuilder.value(p);
                    }
                } else if (TOOLS_ORDER_FIELD.equals(e.getKey())) {
                    for (String t : (String[]) e.getValue()) {
                        xContentBuilder.value(t);
                    }
                } else {
                    for (Map<?, ?> map : (Map<?, ?>[]) e.getValue()) {
                        buildStringToObjectMap(xContentBuilder, map);
                    }
                }
                xContentBuilder.endArray();
            }
        }
        xContentBuilder.endObject();

        return xContentBuilder.endObject();
    }

    /**
     * Parse raw json content into a workflow node instance.
     *
     * @param parser json based content parser
     * @return the parsed WorkflowNode instance
     * @throws IOException if content can't be parsed correctly
     */
    public static WorkflowNode parse(XContentParser parser) throws IOException {
        String id = null;
        String type = null;
        Map<String, String> previousNodeInputs = new HashMap<>();
        Map<String, Object> userInputs = new HashMap<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case ID_FIELD:
                    id = parser.text();
                    break;
                case TYPE_FIELD:
                    type = parser.text();
                    break;
                case PREVIOUS_NODE_INPUTS_FIELD:
                    previousNodeInputs = parseStringToStringMap(parser);
                    break;
                case USER_INPUTS_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String inputFieldName = parser.currentName();
                        switch (parser.nextToken()) {
                            case VALUE_STRING:
                                userInputs.put(inputFieldName, parser.text());
                                break;
                            case START_OBJECT:
                                if (CONFIGURATIONS.equals(inputFieldName)) {
                                    Map<String, Object> configurationsMap = parser.map();
                                    try {
                                        String configurationsString = ParseUtils.parseArbitraryStringToObjectMapToString(configurationsMap);
                                        userInputs.put(inputFieldName, configurationsString);
                                    } catch (Exception ex) {
                                        String errorMessage = "Failed to parse configuration map";
                                        logger.error(errorMessage, ex);
                                        throw new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
                                    }
                                    break;
                                } else {
                                    userInputs.put(inputFieldName, parseStringToStringMap(parser));
                                }
                                break;
                            case START_ARRAY:
                                if (PROCESSORS_FIELD.equals(inputFieldName)) {
                                    List<PipelineProcessor> processorList = new ArrayList<>();
                                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                        processorList.add(PipelineProcessor.parse(parser));
                                    }
                                    userInputs.put(inputFieldName, processorList.toArray(new PipelineProcessor[0]));
                                } else if (TOOLS_ORDER_FIELD.equals(inputFieldName)) {
                                    List<String> toolsList = new ArrayList<>();
                                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                        toolsList.add(parser.text());
                                    }
                                    userInputs.put(inputFieldName, toolsList.toArray(new String[0]));
                                } else {
                                    List<Map<String, Object>> mapList = new ArrayList<>();
                                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                        mapList.add(parseStringToObjectMap(parser));
                                    }
                                    userInputs.put(inputFieldName, mapList.toArray(new Map[0]));
                                }
                                break;
                            case VALUE_NUMBER:
                                switch (parser.numberType()) {
                                    case INT:
                                        userInputs.put(inputFieldName, parser.intValue());
                                        break;
                                    case LONG:
                                        userInputs.put(inputFieldName, parser.longValue());
                                        break;
                                    case FLOAT:
                                        userInputs.put(inputFieldName, parser.floatValue());
                                        break;
                                    case DOUBLE:
                                        userInputs.put(inputFieldName, parser.doubleValue());
                                        break;
                                    case BIG_INTEGER:
                                        userInputs.put(inputFieldName, parser.bigIntegerValue());
                                        break;
                                    default:
                                        throw new FlowFrameworkException(
                                            "Unable to parse field [" + inputFieldName + "] in a node object.",
                                            RestStatus.BAD_REQUEST
                                        );
                                }
                                break;
                            case VALUE_BOOLEAN:
                                userInputs.put(inputFieldName, parser.booleanValue());
                                break;
                            default:
                                throw new FlowFrameworkException(
                                    "Unable to parse field [" + inputFieldName + "] in a node object.",
                                    RestStatus.BAD_REQUEST
                                );
                        }
                    }
                    break;
                default:
                    throw new FlowFrameworkException("Unable to parse field [" + fieldName + "] in a node object.", RestStatus.BAD_REQUEST);
            }
        }
        if (id == null || type == null) {
            throw new FlowFrameworkException("An node object requires both an id and type field.", RestStatus.BAD_REQUEST);
        }

        return new WorkflowNode(id, type, previousNodeInputs, userInputs);
    }

    /**
     * Return this node's id
     * @return the id
     */
    public String id() {
        return id;
    }

    /**
     * Return this node's type
     * @return the type
     */
    public String type() {
        return type;
    }

    /**
     * Return this node's user input data
     * @return the inputs
     */
    public Map<String, Object> userInputs() {
        return userInputs;
    }

    /**
     * Return this node's predecessor inputs
     * @return the inputs
     */
    public Map<String, String> previousNodeInputs() {
        return previousNodeInputs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        WorkflowNode other = (WorkflowNode) obj;
        return Objects.equals(id, other.id);
    }

    @Override
    public String toString() {
        return this.id;
    }
}
