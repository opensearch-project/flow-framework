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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.UI_METADATA_FIELD;
import static org.opensearch.flowframework.common.CommonValue.USER_FIELD;
import static org.opensearch.flowframework.common.CommonValue.VERSION_FIELD;

/**
 * The Template is the central data structure which configures workflows. This object is used to parse JSON communicated via REST API.
 */
public class Template implements ToXContentObject {

    /** The template field name for template workflows */
    public static final String WORKFLOWS_FIELD = "workflows";
    /** The template field name for template compatibility with OpenSearch versions */
    public static final String COMPATIBILITY_FIELD = "compatibility";
    /** The template field name for template version */
    public static final String TEMPLATE_FIELD = "template";
    /** The template field name for template use case */
    public static final String USE_CASE_FIELD = "use_case";

    private String name;
    private String description;
    private String useCase; // probably an ENUM actually
    private Version templateVersion;
    private List<Version> compatibilityVersion;
    private Map<String, Workflow> workflows;
    private Map<String, Object> uiMetadata;
    private User user;

    /**
     * Instantiate the object representing a use case template
     *
     * @param name The template's name
     * @param description A description of the template's use case
     * @param useCase A string defining the internal use case type
     * @param templateVersion The version of this template
     * @param compatibilityVersion OpenSearch version compatibility of this template
     * @param workflows Workflow graph definitions corresponding to the defined operations.
     * @param uiMetadata The UI metadata related to the given workflow
     * @param user The user extracted from the thread context from the request
     */
    public Template(
        String name,
        String description,
        String useCase,
        Version templateVersion,
        List<Version> compatibilityVersion,
        Map<String, Workflow> workflows,
        Map<String, Object> uiMetadata,
        User user
    ) {
        this.name = name;
        this.description = description;
        this.useCase = useCase;
        this.templateVersion = templateVersion;
        this.compatibilityVersion = List.copyOf(compatibilityVersion);
        this.workflows = Map.copyOf(workflows);
        this.uiMetadata = uiMetadata;
        this.user = user;
    }

    private Template() {}

    /**
     * Class for constructing a Builder for Template
     */
    public static class Builder {
        private String name = null;
        private String description = "";
        private String useCase = "";
        private Version templateVersion = null;
        private List<Version> compatibilityVersion = new ArrayList<>();
        private Map<String, Workflow> workflows = new HashMap<>();
        private Map<String, Object> uiMetadata = null;
        private User user = null;

        /**
         * Empty Constructor for the Builder object
         */
        public Builder() {}

        /**
         * Builder method for adding template name
         * @param name template name
         * @return the Builder object
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Builder method for adding template description
         * @param description template description
         * @return the Builder object
         */
        public Builder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Builder method for adding template useCase
         * @param useCase template useCase
         * @return the Builder object
         */
        public Builder useCase(String useCase) {
            this.useCase = useCase;
            return this;
        }

        /**
         * Builder method for adding templateVersion
         * @param templateVersion templateVersion
         * @return the Builder object
         */
        public Builder templateVersion(Version templateVersion) {
            this.templateVersion = templateVersion;
            return this;
        }

        /**
         * Builder method for adding compatibilityVersion
         * @param compatibilityVersion compatibilityVersion
         * @return the Builder object
         */
        public Builder compatibilityVersion(List<Version> compatibilityVersion) {
            this.compatibilityVersion = compatibilityVersion;
            return this;
        }

        /**
         * Builder method for adding workflows
         * @param workflows workflows
         * @return the Builder object
         */
        public Builder workflows(Map<String, Workflow> workflows) {
            this.workflows = workflows;
            return this;
        }

        /**
         * Builder method for adding uiMetadata
         * @param uiMetadata uiMetadata
         * @return the Builder object
         */
        public Builder uiMetadata(Map<String, Object> uiMetadata) {
            this.uiMetadata = uiMetadata;
            return this;
        }

        /**
         * Builder method for adding user
         * @param user user
         * @return the Builder object
         */
        public Builder user(User user) {
            this.user = user;
            return this;
        }

        /**
         * Allows building a template
         * @return Template Object containing all needed fields
         */
        public Template build() {
            Template template = new Template();
            template.name = this.name;
            template.description = this.description;
            template.useCase = this.useCase;
            template.templateVersion = this.templateVersion;
            template.compatibilityVersion = this.compatibilityVersion;
            template.workflows = this.workflows;
            template.uiMetadata = this.uiMetadata;
            template.user = this.user;
            return template;
        }

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

        if (uiMetadata != null && !uiMetadata.isEmpty()) {
            xContentBuilder.field(UI_METADATA_FIELD, uiMetadata);
        }

        if (user != null) {
            xContentBuilder.field(USER_FIELD, user);
        }

        return xContentBuilder.endObject();
    }

    /**
     * Parse raw xContent into a Template instance.
     *
     * @param parser xContent based content parser
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
        Map<String, Object> uiMetadata = null;
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
                                throw new FlowFrameworkException(
                                    "Unable to parse field [" + fieldName + "] in a version object.",
                                    RestStatus.BAD_REQUEST
                                );
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
                case UI_METADATA_FIELD:
                    uiMetadata = parser.map();
                    break;
                case USER_FIELD:
                    user = User.parse(parser);
                    break;
                default:
                    throw new FlowFrameworkException(
                        "Unable to parse field [" + fieldName + "] in a template object.",
                        RestStatus.BAD_REQUEST
                    );
            }
        }
        if (name == null) {
            throw new FlowFrameworkException("A template object requires a name.", RestStatus.BAD_REQUEST);
        }

        return new Builder().name(name)
            .description(description)
            .useCase(useCase)
            .templateVersion(templateVersion)
            .compatibilityVersion(compatibilityVersion)
            .workflows(workflows)
            .uiMetadata(uiMetadata)
            .user(user)
            .build();
    }

    /**
     * Parse a JSON use case template
     *
     * @param json A string containing a JSON representation of a use case template
     * @return A {@link Template} represented by the JSON.
     * @throws IOException on failure to parse
     */
    public static Template parse(String json) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                json
            )
        ) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            return parse(parser);
        }
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
     * A map corresponding to the UI metadata
     * @return the userOutputs
     */
    public Map<String, Object> getUiMetadata() {
        return uiMetadata;
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
            + ", uiMedata="
            + uiMetadata
            + "]";
    }
}
