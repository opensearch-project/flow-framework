/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.get.GetResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Booleans;
import org.opensearch.common.io.Streams;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.ml.repackage.com.google.common.collect.ImmutableList;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;

/**
 * Utility methods for Template parsing
 */
public class ParseUtils {
    private static final Logger logger = LogManager.getLogger(ParseUtils.class);

    // Matches ${{ foo.bar }} (whitespace optional) with capturing groups 1=foo, 2=bar
    private static final Pattern SUBSTITUTION_PATTERN = Pattern.compile("\\$\\{\\{\\s*([\\w_]+)\\.([\\w_]+)\\s*\\}\\}");
    private static final Pattern JSON_ARRAY_DOUBLE_QUOTES_PATTERN = Pattern.compile("\"\\[(.*?)]\"");

    private ParseUtils() {}

    /**
     * Converts a JSON string into an XContentParser
     *
     * @param json the json string
     * @return The XContent parser for the json string
     * @throws IOException on failure to create the parser
     */
    public static XContentParser jsonToParser(String json) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            json
        );
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return parser;
    }

    /**
     * Reads a file from the classpath into a String. Useful for reading JSON mapping files.
     *
     * @param path A string identifying the resource on the class path
     * @return A string containing the contents of the file as UTF-8
     * @throws IOException if file is not found or error reading
     */
    public static String resourceToString(String path) throws IOException {
        try (InputStream is = ParseUtils.class.getResourceAsStream(path)) {
            if (is == null) {
                throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
            }
            final StringBuilder sb = new StringBuilder();
            // Read as UTF-8
            Streams.readAllLines(is, sb::append);
            return sb.toString();
        }
    }

    /**
     * Builds an XContent object representing a map of String keys to String values.
     *
     * @param xContentBuilder An XContent builder whose position is at the start of the map object to build
     * @param map             A map as key-value String pairs.
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
     * 'all_access' role users are treated as admins.
     * @param user of the current role
     * @return boolean if the role is admin
     */
    public static boolean isAdmin(User user) {
        if (user == null) {
            return false;
        }
        return user.getRoles().contains("all_access");
    }

    /**
     * Builds an XContent object representing a map of String keys to Object values.
     *
     * @param xContentBuilder An XContent builder whose position is at the start of the map object to build
     * @param map             A map as key-value String to Object.
     * @throws IOException on a build failure
     */
    public static void buildStringToObjectMap(XContentBuilder xContentBuilder, Map<?, ?> map) throws IOException {
        xContentBuilder.startObject();
        for (Entry<?, ?> e : map.entrySet()) {
            if (e.getValue() instanceof String) {
                xContentBuilder.field((String) e.getKey(), (String) e.getValue());
            } else {
                xContentBuilder.field((String) e.getKey(), e.getValue());
            }
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
     * Parses an XContent object representing a map of String keys to Object values.
     * The Object value here can either be a string or a map
     * If an array is found in the given parser we conver the array to a string representation of the array
     *
     * @param parser An XContent parser whose position is at the start of the map object to parse
     * @return A map as identified by the key-value pairs in the XContent
     * @throws IOException on a parse failure
     */
    public static Map<String, Object> parseStringToObjectMap(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Map<String, Object> map = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                // If the current token is a START_OBJECT, parse it as Map<String, String>
                map.put(fieldName, parseStringToStringMap(parser));
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                // If an array, parse it to a string
                // Handle array: convert it to a string representation
                List<String> elements = new ArrayList<>();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    if (parser.currentToken().equals(XContentParser.Token.VALUE_NUMBER)) {
                        elements.add(String.valueOf(parser.numberValue()));  // If number value don't add escaping quotes
                    } else {
                        elements.add("\"" + parser.text() + "\"");  // Adding escaped quotes around each element
                    }
                }
                map.put(fieldName, elements.toString());
            } else {
                // Otherwise, parse it as a string
                map.put(fieldName, parser.text());
            }
        }
        return map;
    }

    /**
     * Parse content parser to {@link java.time.Instant}.
     *
     * @param parser json based content parser
     * @return instance of {@link java.time.Instant}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Instant parseInstant(XContentParser parser) throws IOException {
        if (parser.currentToken() != null && parser.currentToken().isValue() && parser.currentToken() != XContentParser.Token.VALUE_NULL) {
            return Instant.ofEpochMilli(parser.longValue());
        }
        return null;
    }

    /**
     * Generates a user string formed by the username, backend roles, roles and requested tenants separated by '|'
     * (e.g., john||own_index,testrole|__user__, no backend role so you see two verticle line after john.).
     * This is the user string format used internally in the OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT and may be
     * parsed using User.parse(string).
     *
     * @param client Client containing user info. A public API request will fill in the user info in the thread context.
     * @return parsed user object
     */
    public static User getUserContext(Client client) {
        String userStr = client.threadPool().getThreadContext().getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
        logger.debug("Filtering result by {}", userStr);
        return User.parse(userStr);
    }

    /**
     * Checks whether resource authz framework should be used.
     * @param resourceType the type to check for resource authz
     * @return true if resource-authz should be used, false otherwise
     */
    public static boolean shouldUseResourceAuthz(String resourceType) {
        var client = ResourceSharingClientAccessor.getInstance().getResourceSharingClient();
        return client != null && client.isFeatureEnabledForType(resourceType);
    }

    /**
     * Verifies whether the user has permission to access the resource.
     * @param resourceType the type of resource to be authorized
     * @param onSuccess consumer function to execute if resource sharing feature is enabled
     * @param fallBackIfDisabled consumer function to execute if resource sharing feature is disabled.
     */
    public static void verifyResourceAccessAndProcessRequest(String resourceType, Runnable onSuccess, Runnable fallBackIfDisabled) {
        // Resource access will be auto-evaluated
        if (shouldUseResourceAuthz(resourceType)) {
            onSuccess.run();
        } else {
            fallBackIfDisabled.run();
        }
    }

    /**
     * Add user backend roles filter to search source builder=
     * @param user the user
     * @param searchSourceBuilder search builder
     * @return search builder with filter added
     */
    public static SearchSourceBuilder addUserBackendRolesFilter(User user, SearchSourceBuilder searchSourceBuilder) {
        if (user == null) {
            return searchSourceBuilder;
        }
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        String userFieldName = "user";
        String userBackendRoleFieldName = "user.backend_roles.keyword";
        List<String> backendRoles = user.getBackendRoles() != null ? user.getBackendRoles() : ImmutableList.of();
        // For normal case, user should have backend roles.
        TermsQueryBuilder userRolesFilterQuery = QueryBuilders.termsQuery(userBackendRoleFieldName, backendRoles);
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(userFieldName, userRolesFilterQuery, ScoreMode.None);
        boolQueryBuilder.must(nestedQueryBuilder);
        QueryBuilder query = searchSourceBuilder.query();
        if (query == null) {
            searchSourceBuilder.query(boolQueryBuilder);
        } else if (query instanceof BoolQueryBuilder) {
            ((BoolQueryBuilder) query).filter(boolQueryBuilder);
        } else {
            // Convert any other query to a BoolQueryBuilder
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().must(query);
            boolQuery.filter(boolQueryBuilder);
            searchSourceBuilder.query(boolQuery);
        }
        return searchSourceBuilder;
    }

    /**
     * Resolve user and execute the function
     * @param requestedUser the user to execute the request
     * @param workflowId workflow id
     * @param tenantId tenant id
     * @param filterByEnabled filter by enabled setting
     * @param statePresent state present for the transport action
     * @param isMultitenancyEnabled whether multitenancy is enabled
     * @param listener action listener
     * @param function workflow function
     * @param client node client
     * @param sdkClient multitenant client
     * @param clusterService cluster service
     * @param xContentRegistry contentRegister to parse get response
     */
    public static void resolveUserAndExecute(
        User requestedUser,
        String workflowId,
        String tenantId,
        Boolean filterByEnabled,
        Boolean statePresent,
        boolean isMultitenancyEnabled,
        ActionListener<? extends ActionResponse> listener,
        Runnable function,
        Client client,
        SdkClient sdkClient,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry
    ) {
        try {
            if (!isMultitenancyEnabled && (requestedUser == null || filterByEnabled == Boolean.FALSE)) {
                // requestedUser == null means security is disabled or user is superadmin. In this case we don't need to
                // check if request user have access to the workflow or not unless we have multitenancy
                // !filterByEnabled means security is enabled and filterByEnabled is disabled
                function.run();
            } else {
                // we need to validate either user access, multitenancy, or both, which requires getting the workflow
                getWorkflow(
                    requestedUser,
                    workflowId,
                    tenantId,
                    filterByEnabled,
                    statePresent,
                    isMultitenancyEnabled,
                    listener,
                    function,
                    client,
                    sdkClient,
                    clusterService,
                    xContentRegistry
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Check if requested user has backend role required to access the resource
     * @param requestedUser the user to execute the request
     * @param resourceUser user of the resource
     * @param workflowId workflow id
     * @return boolean if the requested user has backend role required to access the resource
     * @throws Exception exception
     */
    private static boolean checkUserPermissions(User requestedUser, User resourceUser, String workflowId) throws Exception {
        if (requestedUser == null || resourceUser == null) {
            return false;
        }
        if (resourceUser.getBackendRoles() == null || requestedUser.getBackendRoles() == null) {
            return false;
        }
        // Check if requested user has backend role required to access the resource
        for (String backendRole : requestedUser.getBackendRoles()) {
            if (resourceUser.getBackendRoles().contains(backendRole)) {
                logger.debug(
                    "User: "
                        + requestedUser.getName()
                        + " has backend role: "
                        + backendRole
                        + " permissions to access config: "
                        + workflowId
                );
                return true;
            }
        }
        return false;
    }

    /**
     * Check if filter by backend roles is enabled and validate the requested user
     * @param requestedUser the user to execute the request
     */
    public static void checkFilterByBackendRoles(User requestedUser) {
        if (requestedUser == null) {
            String errorMessage = "Filter by backend roles is enabled and User is null";
            logger.error(errorMessage);
            throw new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
        }
        if (requestedUser.getBackendRoles().isEmpty()) {
            String userErrorMessage = "Filter by backend roles is enabled, but User "
                + requestedUser.getName()
                + " does not have any backend roles configured";

            logger.error(userErrorMessage);
            throw new FlowFrameworkException(userErrorMessage, RestStatus.FORBIDDEN);
        }
    }

    /**
     * Get workflow
     * @param requestUser the user to execute the request
     * @param workflowId workflow id
     * @param tenantId tenant id
     * @param filterByEnabled filter by enabled setting
     * @param statePresent state present for the transport action
     * @param isMultitenancyEnabled if multi tenancy is enabled
     * @param listener action listener
     * @param function workflow function
     * @param client node client
     * @param sdkClient the tenant aware client
     * @param clusterService cluster service
     * @param xContentRegistry contentRegister to parse get response
     */
    public static void getWorkflow(
        User requestUser,
        String workflowId,
        String tenantId,
        Boolean filterByEnabled,
        Boolean statePresent,
        boolean isMultitenancyEnabled,
        ActionListener<? extends ActionResponse> listener,
        Runnable function,
        Client client,
        SdkClient sdkClient,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry
    ) {
        String index = statePresent ? WORKFLOW_STATE_INDEX : GLOBAL_CONTEXT_INDEX;
        if (FlowFrameworkIndicesHandler.doesIndexExistMultitenant(clusterService, index, isMultitenancyEnabled)) {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                GetDataObjectRequest request = GetDataObjectRequest.builder().index(index).id(workflowId).tenantId(tenantId).build();
                sdkClient.getDataObjectAsync(request).whenComplete((r, throwable) -> {
                    if (throwable == null) {
                        try {
                            GetResponse getResponse = r.parser() == null ? null : GetResponse.fromXContent(r.parser());
                            onGetWorkflowResponse(
                                getResponse,
                                requestUser,
                                workflowId,
                                tenantId,
                                filterByEnabled,
                                statePresent,
                                isMultitenancyEnabled,
                                listener,
                                function,
                                xContentRegistry,
                                context
                            );
                        } catch (IOException e) {
                            logger.error("Failed to parse workflow getResponse: {}", workflowId, e);
                            listener.onFailure(e);
                        }
                    } else {
                        Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                        logger.error("Failed to get workflow: {}", workflowId, exception);
                        listener.onFailure(exception);
                    }
                });
            }
        } else {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Failed to retrieve template ({}).", workflowId)
                .getFormattedMessage();
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
        }
    }

    /**
     * Execute the function if user has permissions to access the resource
     * @param response get response
     * @param requestUser the user to execute the request
     * @param workflowId workflow id
     * @param tenantId tenant id
     * @param filterByEnabled filter by enabled setting
     * @param statePresent state present for the transport action
     * @param isMultitenancyEnabled if multi tenancy is enabled
     * @param listener action listener
     * @param function workflow function
     * @param xContentRegistry contentRegister to parse get response
     * @param context thread context
     */
    public static void onGetWorkflowResponse(
        GetResponse response,
        User requestUser,
        String workflowId,
        String tenantId,
        Boolean filterByEnabled,
        Boolean statePresent,
        boolean isMultitenancyEnabled,
        ActionListener<? extends ActionResponse> listener,
        Runnable function,
        NamedXContentRegistry xContentRegistry,
        ThreadContext.StoredContext context
    ) {
        if (response.isExists()) {
            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                context.restore();
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                User resourceUser;
                if (statePresent) {
                    WorkflowState state = WorkflowState.parse(parser);
                    resourceUser = state.getUser();
                    if (!TenantAwareHelper.validateTenantResource(isMultitenancyEnabled, tenantId, state.getTenantId(), listener)) {
                        return;
                    }
                } else {
                    Template template = Template.parse(parser);
                    resourceUser = template.getUser();
                    if (!TenantAwareHelper.validateTenantResource(isMultitenancyEnabled, tenantId, template.getTenantId(), listener)) {
                        return;
                    }
                }
                if (shouldUseResourceAuthz(CommonValue.WORKFLOW_RESOURCE_TYPE)
                    || !filterByEnabled
                    || isAdmin(requestUser)
                    || checkUserPermissions(requestUser, resourceUser, workflowId)) {
                    function.run();
                } else {
                    logger.debug("User: " + requestUser.getName() + " does not have permissions to access workflow: " + workflowId);
                    listener.onFailure(
                        new FlowFrameworkException("User does not have permissions to access workflow: " + workflowId, RestStatus.FORBIDDEN)
                    );
                }
            } catch (Exception e) {
                logger.error("Failed to parse workflow: {}", workflowId, e);
                listener.onFailure(e);
            }
        } else {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to retrieve template ({}) from global context.",
                workflowId
            ).getFormattedMessage();
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
        }
    }

    /**
     * Creates a XContentParser from a given Registry
     *
     * @param xContentRegistry main registry for serializable content
     * @param bytesReference   given bytes to be parsed
     * @return bytesReference of {@link java.time.Instant}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static XContentParser createXContentParserFromRegistry(NamedXContentRegistry xContentRegistry, BytesReference bytesReference)
        throws IOException {
        return XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON);
    }

    /**
     * Generates a string to string Map
     *
     * @param map       content map
     * @param fieldName fieldName
     * @return instance of the map
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> getStringToStringMap(Object map, String fieldName) {
        if (map instanceof Map) {
            return (Map<String, String>) map;
        }
        throw new IllegalArgumentException("[" + fieldName + "] must be a key-value map.");
    }

    /**
     * Creates a map containing the specified input keys, with values derived from template data or previous node
     * output.
     *
     * @param requiredInputKeys  A set of keys that must be present, or will cause an exception to be thrown
     * @param optionalInputKeys  A set of keys that may be present, or will be absent in the returned map
     * @param currentNodeInputs  Input params and content for this node, from workflow parsing
     * @param outputs            WorkflowData content of previous steps
     * @param previousNodeInputs Input params for this node that come from previous steps
     * @param params             Params that came from REST path
     * @return A map containing the requiredInputKeys with their corresponding values,
     * and optionalInputKeys with their corresponding values if present.
     * Throws a {@link FlowFrameworkException} if a required key is not present.
     */
    public static Map<String, Object> getInputsFromPreviousSteps(
        Set<String> requiredInputKeys,
        Set<String> optionalInputKeys,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        // Mutable set to ensure all required keys are used
        Set<String> requiredKeys = new HashSet<>(requiredInputKeys);
        // Merge input sets to add all requested keys
        Set<String> keys = new HashSet<>(requiredInputKeys);
        keys.addAll(optionalInputKeys);
        // Initialize return map
        Map<String, Object> inputs = new HashMap<>();
        for (String key : keys) {
            Object value = null;
            // Priority 1: specifically named prior step inputs
            // ... parse the previousNodeInputs map and fill in the specified keys
            Optional<String> previousNodeForKey = previousNodeInputs.entrySet()
                .stream()
                .filter(e -> key.equals(e.getValue()))
                .map(Map.Entry::getKey)
                .findAny();
            if (previousNodeForKey.isPresent()) {
                WorkflowData previousNodeOutput = outputs.get(previousNodeForKey.get());
                if (previousNodeOutput != null) {
                    value = previousNodeOutput.getContent().get(key);
                }
            }
            // Priority 2: inputs specified in template
            // ... fetch from currentNodeInputs (params take precedence)
            if (value == null) {
                value = currentNodeInputs.getParams().get(key);
            }
            if (value == null) {
                value = currentNodeInputs.getContent().get(key);
            }
            // Priority 3: other inputs
            if (value == null) {
                Optional<Object> matchedValue = outputs.values()
                    .stream()
                    .map(WorkflowData::getContent)
                    .filter(m -> m.containsKey(key))
                    .map(m -> m.get(key))
                    .findAny();
                if (matchedValue.isPresent()) {
                    value = matchedValue.get();
                }
            }
            if (value != null) {
                // Check for any substitution(s) in value, list, or map
                if (value instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> valueMap = (Map<String, Object>) value;
                    value = valueMap.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> conditionallySubstitute(e.getValue(), outputs, params)));
                } else if (value instanceof List) {
                    value = ((List<?>) value).stream().map(v -> conditionallySubstitute(v, outputs, params)).collect(Collectors.toList());
                } else {
                    value = conditionallySubstitute(value, outputs, params);
                }
                // Add value to inputs and mark that a required key was present
                inputs.put(key, value);
                requiredKeys.remove(key);
            }
        }
        // After iterating is complete, throw exception if requiredKeys is not empty
        if (!requiredKeys.isEmpty()) {
            throw new FlowFrameworkException(
                "Missing required inputs "
                    + requiredKeys
                    + " in workflow ["
                    + currentNodeInputs.getWorkflowId()
                    + "] node ["
                    + currentNodeInputs.getNodeId()
                    + "]",
                RestStatus.BAD_REQUEST
            );
        }
        // Finally return the map
        return inputs;
    }

    /**
     * Executes substitution on the given value by looking at any matching values in either the ouputs or params map
     *
     * @param value   the Object that will have the substitution done on
     * @param outputs potential location of values to be substituted in
     * @param params  potential location of values to be subsituted in
     * @return the substituted object back
     */
    public static Object conditionallySubstitute(Object value, Map<String, WorkflowData> outputs, Map<String, String> params) {
        if (value instanceof String) {
            Matcher m = SUBSTITUTION_PATTERN.matcher((String) value);
            StringBuilder result = new StringBuilder();
            while (m.find() && outputs != null) {
                // outputs content map contains values for previous node input (e.g: deploy_openai_model.model_id)
                // Check first if the substitution is looking for the same key, value pair and if yes
                // then replace it with the key value pair in the inputs map
                String replacement = m.group(0);
                if (outputs.containsKey(m.group(1)) && outputs.get(m.group(1)).getContent().containsKey(m.group(2))) {
                    // Extract the key for the inputs (e.g., "model_id" from ${{deploy_openai_model.model_id}})
                    String key = m.group(2);
                    if (outputs.get(m.group(1)).getContent().get(key) instanceof String) {
                        replacement = (String) outputs.get(m.group(1)).getContent().get(key);
                        // Replace the whole sequence with the value from the map
                        m.appendReplacement(result, Matcher.quoteReplacement(replacement));
                    }
                }
            }
            m.appendTail(result);
            value = result.toString();

            if (params != null) {
                for (Map.Entry<String, String> e : params.entrySet()) {
                    String regex = "\\$\\{\\{\\s*" + Pattern.quote(e.getKey()) + "\\s*\\}\\}";
                    String replacement = e.getValue();

                    // Correctly escape backslashes, newlines, and quotes for JSON compatibility
                    replacement = replacement.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");

                    // Use Matcher.quoteReplacement to handle special replacement characters like $ and \ that weren't previously handled
                    replacement = Matcher.quoteReplacement(replacement);
                    value = ((String) value).replaceAll(regex, replacement);
                }
            }
        }
        return value;
    }

    /**
     * Generates a string based on an arbitrary String to object map using Jackson
     *
     * @param map content map
     * @return instance of the string
     * @throws Exception for issues processing map
     */
    public static String parseArbitraryStringToObjectMapToString(Map<String, Object> map) throws Exception {
        try (Jsonb jsonb = JsonbBuilder.create()) {
            return jsonb.toJson(map);
        }
    }

    /**
     * Generates a String to String map based on a Json File
     *
     * @param path file path
     * @return instance of the string
     * @throws Exception for issues processing map
     */
    public static Map<String, String> parseJsonFileToStringToStringMap(String path) throws Exception {
        String jsonContent = resourceToString(path);
        try (Jsonb jsonb = JsonbBuilder.create()) {
            @SuppressWarnings("unchecked")
            Map<String, String> resultMap = jsonb.fromJson(jsonContent, Map.class);
            return resultMap;
        }
    }

    /**
     * Takes an input string, then checks if there is an array in the string with backslashes around strings
     * (e.g.  "[\"text\", \"hello\"]" to "["text", "hello"]"), this is needed for processors that take in string arrays,
     * This also removes the quotations around the array making the array valid to consume
     * (e.g. "weights": "[0.7, 0.3]" to "weights": [0.7, 0.3])
     *
     * @param input The inputString given to be transformed
     * @return the transformed string
     */
    public static String removingBackslashesAndQuotesInArrayInJsonString(String input) {
        Matcher matcher = JSON_ARRAY_DOUBLE_QUOTES_PATTERN.matcher(input);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            // Extract matched content and remove backslashes before quotes
            String withoutEscapes = matcher.group(1).replaceAll("\\\\\"", "\"");
            // Return the transformed string with the brackets but without the outer quotes
            matcher.appendReplacement(result, "[" + withoutEscapes + "]");
        }
        // Append remaining input after the last match
        matcher.appendTail(result);
        return result.toString();
    }

    /**
     * Takes a String to json object map and converts this to a String to String map
     * @param stringToObjectMap The string to object map to be transformed
     * @return the transformed map
     * @throws Exception for issues processing map
     */
    public static Map<String, String> convertStringToObjectMapToStringToStringMap(Map<String, Object> stringToObjectMap) throws Exception {
        try (Jsonb jsonb = JsonbBuilder.create()) {
            Map<String, String> stringToStringMap = new HashMap<>();
            for (Map.Entry<String, Object> entry : stringToObjectMap.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof String) {
                    stringToStringMap.put(entry.getKey(), (String) value);
                } else {
                    stringToStringMap.put(entry.getKey(), jsonb.toJson(value));
                }
            }
            return stringToStringMap;
        }
    }

    /**
     * Checks if the inputs map contains the specified key and parses the associated value to a generic class.
     *
     * @param <T> the type to which the value should be parsed
     * @param inputs the map containing the input data
     * @param key the key to check in the map
     * @param type the class to parse the value to
     * @throws IllegalArgumentException if the type is not supported
     * @return the generic type value associated with the key if present, or null if the key is not found
     */
    public static <T> T parseIfExists(Map<String, Object> inputs, String key, Class<T> type) {
        if (!inputs.containsKey(key)) {
            return null;
        }

        Object value = inputs.get(key);
        if (type == Boolean.class) {
            return type.cast(Booleans.parseBoolean(value.toString()));
        } else if (type == Float.class) {
            return type.cast(Float.parseFloat(value.toString()));
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    /**
     * Compares workflow node user inputs
     * @param originalInputs the original node user inputs
     * @param updatedInputs the updated node user inputs
     * @throws Exception for issues processing map
     * @return boolean if equivalent
     */
    public static boolean userInputsEquals(Map<String, Object> originalInputs, Map<String, Object> updatedInputs) throws Exception {
        String originalInputsJson = parseArbitraryStringToObjectMapToString(originalInputs);
        String updatedInputsJson = parseArbitraryStringToObjectMapToString(updatedInputs);
        JsonElement elem1 = JsonParser.parseString(originalInputsJson);
        JsonElement elem2 = JsonParser.parseString(updatedInputsJson);
        return elem1.equals(elem2);
    }

    /**
     * Flattens a nested map of settings, delimitted by a period
     * @param prefix the setting prefix
     * @param settings the nested setting map
     * @param flattenedSettings the final flattend map of settings
     */
    public static void flattenSettings(String prefix, Map<String, Object> settings, Map<String, Object> flattenedSettings) {
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> mapValue = (Map<String, Object>) value;
                flattenSettings(key, mapValue, flattenedSettings);
            } else {
                flattenedSettings.put(key, value.toString());
            }
        }
    }

    /**
     * Ensures index is prepended to flattened setting keys
     * @param originalSettings the original settings map
     * @return new map with keys prepended with index
     */
    public static Map<String, Object> prependIndexToSettings(Map<String, Object> originalSettings) {
        Map<String, Object> newSettings = new HashMap<>();
        originalSettings.entrySet().stream().forEach(x -> {
            if (!x.getKey().startsWith("index.")) {
                newSettings.put("index." + x.getKey(), x.getValue());
            } else {
                newSettings.put(x.getKey(), x.getValue());
            }
        });
        return newSettings;
    }
}
