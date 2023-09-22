/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.Version;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.yaml.YamlXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.workflow.Workflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * The Template is the central data structure which configures workflows. This object is used to parse JSON communicated via REST API.
 */
public class Template implements ToXContentObject {

    /** The template field name for template name */
    public static final String NAME_FIELD = "name";
    /** The template field name for template description */
    public static final String DESCRIPTION_FIELD = "description";
    /** The template field name for template use case */
    public static final String USE_CASE_FIELD = "use_case";
    /** The template field name for template operations */
    public static final String OPERATIONS_FIELD = "operations";
    /** The template field name for template version information */
    public static final String VERSION_FIELD = "version";
    /** The template field name for template version */
    public static final String TEMPLATE_FIELD = "template";
    /** The template field name for template compatibility with OpenSearch versions */
    public static final String COMPATIBILITY_FIELD = "compatibility";
    /** The template field name for template user inputs */
    public static final String USER_INPUTS_FIELD = "user_inputs";
    /** The template field name for template workflows */
    public static final String WORKFLOWS_FIELD = "workflows";

    private final String name;
    private final String description;
    private final String useCase; // probably an ENUM actually
    private final List<String> operations; // probably an ENUM actually
    private final Version templateVersion;
    private final List<Version> compatibilityVersion;
    private final Map<String, Object> userInputs;
    private final Map<String, Workflow> workflows;

    /**
     * Instantiate the object representing a use case template
     *
     * @param name The template's name
     * @param description A description of the template's use case
     * @param useCase A string defining the internal use case type
     * @param operations Expected operations of this template. Should match defined workflows.
     * @param templateVersion The version of this template
     * @param compatibilityVersion OpenSearch version compatibility of this template
     * @param userInputs Optional user inputs to apply globally
     * @param workflows Workflow graph definitions corresponding to the defined operations.
     */
    public Template(
        String name,
        String description,
        String useCase,
        List<String> operations,
        Version templateVersion,
        List<Version> compatibilityVersion,
        Map<String, Object> userInputs,
        Map<String, Workflow> workflows
    ) {
        this.name = name;
        this.description = description;
        this.useCase = useCase;
        this.operations = List.copyOf(operations);
        this.templateVersion = templateVersion;
        this.compatibilityVersion = List.copyOf(compatibilityVersion);
        this.userInputs = Map.copyOf(userInputs);
        this.workflows = Map.copyOf(workflows);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field(NAME_FIELD, this.name);
        xContentBuilder.field(DESCRIPTION_FIELD, this.description);
        xContentBuilder.field(USE_CASE_FIELD, this.useCase);
        xContentBuilder.startArray(OPERATIONS_FIELD);
        for (String op : this.operations) {
            xContentBuilder.value(op);
        }
        xContentBuilder.endArray();

        if (this.templateVersion != null || !this.compatibilityVersion.isEmpty()) {
            xContentBuilder.startObject(VERSION_FIELD);
            if (this.templateVersion != null) {
                xContentBuilder.field(TEMPLATE_FIELD, this.templateVersion);
            }
            if (!this.compatibilityVersion.isEmpty()) {
                xContentBuilder.startArray(COMPATIBILITY_FIELD);
                for (Version v : this.compatibilityVersion) {
                    xContentBuilder.value(v);
                }
                xContentBuilder.endArray();
            }
            xContentBuilder.endObject();
        }

        if (!this.userInputs.isEmpty()) {
            xContentBuilder.startObject(USER_INPUTS_FIELD);
            for (Entry<String, Object> e : userInputs.entrySet()) {
                xContentBuilder.field(e.getKey(), e.getValue());
            }
            xContentBuilder.endObject();
        }

        xContentBuilder.startObject(WORKFLOWS_FIELD);
        for (Entry<String, Workflow> e : workflows.entrySet()) {
            xContentBuilder.field(e.getKey(), e.getValue(), params);
        }
        xContentBuilder.endObject();

        return xContentBuilder.endObject();
    }

    /**
     * Parse raw json content into a workflow node instance.
     *
     * @param parser json based content parser
     * @return an instance of the template
     * @throws IOException if content can't be parsed correctly
     */
    public static Template parse(XContentParser parser) throws IOException {
        String name = null;
        String description = "";
        String useCase = "";
        List<String> operations = new ArrayList<>();
        Version templateVersion = null;
        List<Version> compatibilityVersion = new ArrayList<>();
        Map<String, Object> userInputs = new HashMap<>();
        Map<String, Workflow> workflows = new HashMap<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case NAME_FIELD:
                    name = parser.text();
                    break;
                case DESCRIPTION_FIELD:
                    description = parser.text();
                    break;
                case USE_CASE_FIELD:
                    useCase = parser.text();
                    break;
                case OPERATIONS_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        operations.add(parser.text());
                    }
                    break;
                case VERSION_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String versionFieldName = parser.currentName();
                        parser.nextToken();
                        switch (versionFieldName) {
                            case TEMPLATE_FIELD:
                                templateVersion = Version.fromString(parser.text());
                                break;
                            case COMPATIBILITY_FIELD:
                                ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    compatibilityVersion.add(Version.fromString(parser.text()));
                                }
                                break;
                            default:
                                throw new IOException("Unable to parse field [" + fieldName + "] in a version object.");
                        }
                    }
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
                                userInputs.put(inputFieldName, parseStringToStringMap(parser));
                                break;
                            default:
                                throw new IOException("Unable to parse field [" + inputFieldName + "] in a user inputs object.");
                        }
                    }
                    break;
                case WORKFLOWS_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String workflowFieldName = parser.currentName();
                        parser.nextToken();
                        workflows.put(workflowFieldName, Workflow.parse(parser));
                    }
                    break;

                default:
                    throw new IOException("Unable to parse field [" + fieldName + "] in a template object.");
            }
        }
        if (name == null) {
            throw new IOException("An template object requires a name.");
        }

        return new Template(name, description, useCase, operations, templateVersion, compatibilityVersion, userInputs, workflows);
    }

    /**
     * Parse a JSON use case template
     *
     * @param json A string containing a JSON representation of a use case template
     * @return A {@link Template} represented by the JSON.
     * @throws IOException on failure to parse
     */
    public static Template parse(String json) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            json
        );
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return parse(parser);
    }

    /**
     * Builds an XContent object representing a map of String keys to String values.
     *
     * @param xContentBuilder An XContent builder whose position is at the start of the map object to build
     * @param map A map as key-value String pairs.
     * @throws IOException on a build failure
     */
    public static void buildStringToStringMap(XContentBuilder xContentBuilder, Map<?, ?> map) throws IOException {
        xContentBuilder.startObject();
        for (Entry<?, ?> e : map.entrySet()) {
            xContentBuilder.field((String) e.getKey(), (String) e.getValue());
        }
        xContentBuilder.endObject();
    }

    /**
     * Parses an XContent object representing a map of String keys to String values.
     *
     * @param parser An XContent parser whose position is at the start of the map object to parse
     * @return A map as identified by the key-value pairs in the XContent
     * @throws IOException on a parse failure
     */
    public static Map<String, String> parseStringToStringMap(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Map<String, String> map = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            map.put(fieldName, parser.text());
        }
        return map;
    }

    /**
     * Output this object in a compact JSON string.
     *
     * @return a JSON representation of the template.
     */
    public String toJson() {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            return this.toXContent(builder, EMPTY_PARAMS).toString();
        } catch (IOException e) {
            return "{\"error\": \"couldn't create JSON: " + e.getMessage() + "\"}";
        }
    }

    /**
     * Output this object in YAML.
     *
     * @return a YAML representation of the template.
     */
    public String toYaml() {
        try {
            XContentBuilder builder = YamlXContent.contentBuilder();
            return this.toXContent(builder, EMPTY_PARAMS).toString();
        } catch (IOException e) {
            return "error: couldn't create YAML: " + e.getMessage();
        }
    }

    /**
     * The name of this template
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * A description of what this template does
     * @return the description
     */
    public String description() {
        return description;
    }

    /**
     * A canonical use case name for this template
     * @return the useCase
     */
    public String useCase() {
        return useCase;
    }

    /**
     * Operations this use case supports
     * @return the operations
     */
    public List<String> operations() {
        return operations;
    }

    /**
     * The version of this template
     * @return the templateVersion
     */
    public Version templateVersion() {
        return templateVersion;
    }

    /**
     * OpenSearch version compatibility of this template
     * @return the compatibilityVersion
     */
    public List<Version> compatibilityVersion() {
        return compatibilityVersion;
    }

    /**
     * A map of user inputs
     * @return the userInputs
     */
    public Map<String, Object> userInputs() {
        return userInputs;
    }

    /**
     * Workflows encoded in this template, generally corresponding to the operations returned by {@link #operations()}.
     * @return the workflows
     */
    public Map<String, Workflow> workflows() {
        return workflows;
    }

    @Override
    public String toString() {
        return "Template [name="
            + name
            + ", description="
            + description
            + ", useCase="
            + useCase
            + ", operations="
            + operations
            + ", templateVersion="
            + templateVersion
            + ", compatibilityVersion="
            + compatibilityVersion
            + ", userInputs="
            + userInputs
            + ", workflows="
            + workflows
            + "]";
    }
}
