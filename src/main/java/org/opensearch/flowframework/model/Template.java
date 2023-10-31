/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.Version;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.yaml.YamlXContent;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.COMPATIBILITY_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TEMPLATE_FIELD;
import static org.opensearch.flowframework.common.CommonValue.USER_FIELD;
import static org.opensearch.flowframework.common.CommonValue.USE_CASE_FIELD;
import static org.opensearch.flowframework.common.CommonValue.VERSION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOWS_FIELD;

/**
 * The Template is the central data structure which configures workflows. This object is used to parse JSON communicated via REST API.
 */
public class Template implements ToXContentObject {

    private final String name;
    private final String description;
    private final String useCase; // probably an ENUM actually
    private final Version templateVersion;
    private final List<Version> compatibilityVersion;
    private final Map<String, Workflow> workflows;
    private final User user;

    /**
     * Instantiate the object representing a use case template
     *
     * @param name The template's name
     * @param description A description of the template's use case
     * @param useCase A string defining the internal use case type
     * @param templateVersion The version of this template
     * @param compatibilityVersion OpenSearch version compatibility of this template
     * @param workflows Workflow graph definitions corresponding to the defined operations.
     * @param user The user extracted from the thread context from the request
     */
    public Template(
        String name,
        String description,
        String useCase,
        Version templateVersion,
        List<Version> compatibilityVersion,
        Map<String, Workflow> workflows,
        User user
    ) {
        this.name = name;
        this.description = description;
        this.useCase = useCase;
        this.templateVersion = templateVersion;
        this.compatibilityVersion = List.copyOf(compatibilityVersion);
        this.workflows = Map.copyOf(workflows);
        this.user = user;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field(NAME_FIELD, this.name);
        xContentBuilder.field(DESCRIPTION_FIELD, this.description);
        xContentBuilder.field(USE_CASE_FIELD, this.useCase);

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

        xContentBuilder.startObject(WORKFLOWS_FIELD);
        for (Entry<String, Workflow> e : workflows.entrySet()) {
            xContentBuilder.field(e.getKey(), e.getValue(), params);
        }
        xContentBuilder.endObject();
        if (user != null) {
            xContentBuilder.field(USER_FIELD, user);
        }

        return xContentBuilder.endObject();
    }

    /**
     * Parse raw json content into a Template instance.
     *
     * @param parser json based content parser
     * @return an instance of the template
     * @throws IOException if content can't be parsed correctly
     */
    public static Template parse(XContentParser parser) throws IOException {
        String name = null;
        String description = "";
        String useCase = "";
        Version templateVersion = null;
        List<Version> compatibilityVersion = new ArrayList<>();
        Map<String, Workflow> workflows = new HashMap<>();
        User user = null;

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
                case WORKFLOWS_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String workflowFieldName = parser.currentName();
                        parser.nextToken();
                        workflows.put(workflowFieldName, Workflow.parse(parser));
                    }
                    break;
                case USER_FIELD:
                    user = User.parse(parser);
                    break;
                default:
                    throw new IOException("Unable to parse field [" + fieldName + "] in a template object.");
            }
        }
        if (name == null) {
            throw new IOException("An template object requires a name.");
        }

        return new Template(name, description, useCase, templateVersion, compatibilityVersion, workflows, user);
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
     * Workflows encoded in this template
     * @return the workflows
     */
    public Map<String, Workflow> workflows() {
        return workflows;
    }

    /**
     * User that created and owns this template
     * @return the user
     */
    public User getUser() {
        return user;
    }

    @Override
    public String toString() {
        return "Template [name="
            + name
            + ", description="
            + description
            + ", useCase="
            + useCase
            + ", templateVersion="
            + templateVersion
            + ", compatibilityVersion="
            + compatibilityVersion
            + ", workflows="
            + workflows
            + "]";
    }
}
