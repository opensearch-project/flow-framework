/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.flowframework.common.FlowFrameworkFeatureEnabledSetting;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;
import static org.opensearch.flowframework.util.RestHandlerUtils.getSourceContext;

/**
 * Abstract class to handle search request.
 * @param <T> The type to search
 */
public abstract class AbstractSearchWorkflowAction<T extends ToXContentObject> extends BaseRestHandler {

    /** Url Paths of the routes*/
    protected final List<String> urlPaths;
    /** Index on search operation needs to be performed*/
    protected final String index;
    /** Search class name*/
    protected final Class<T> clazz;
    /** Search action type*/
    protected final ActionType<SearchResponse> actionType;
    /** Settings to enable FlowFramework API*/
    protected final FlowFrameworkFeatureEnabledSetting flowFrameworkFeatureEnabledSetting;

    /**
     * Instantiates a new AbstractSearchWorkflowAction
     * @param urlPaths urlPaths to create routes
     * @param index index the search should be done on
     * @param clazz model class
     * @param actionType from which action abstract class is called
     * @param flowFrameworkFeatureEnabledSetting Whether this API is enabled
     */
    public AbstractSearchWorkflowAction(
        List<String> urlPaths,
        String index,
        Class<T> clazz,
        ActionType<SearchResponse> actionType,
        FlowFrameworkFeatureEnabledSetting flowFrameworkFeatureEnabledSetting
    ) {
        this.urlPaths = urlPaths;
        this.index = index;
        this.clazz = clazz;
        this.actionType = actionType;
        this.flowFrameworkFeatureEnabledSetting = flowFrameworkFeatureEnabledSetting;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()) {
            FlowFrameworkException ffe = new FlowFrameworkException(
                "This API is disabled. To enable it, update the setting [" + FLOW_FRAMEWORK_ENABLED.getKey() + "] to true.",
                RestStatus.FORBIDDEN
            );
            return channel -> channel.sendResponse(
                new BytesRestResponse(ffe.getRestStatus(), ffe.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser());
        searchSourceBuilder.fetchSource(getSourceContext(request, searchSourceBuilder));
        searchSourceBuilder.seqNoAndPrimaryTerm(true).version(true);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(index);
        return channel -> client.execute(actionType, searchRequest, search(channel));
    }

    /**
     * Builds the action response for the Search Request
     *
     * @param channel the REST channel
     * @return the action response
     */
    protected RestResponseListener<SearchResponse> search(RestChannel channel) {
        return new RestResponseListener<SearchResponse>(channel) {
            @Override
            public RestResponse buildResponse(SearchResponse response) throws Exception {
                if (response.isTimedOut()) {
                    return new BytesRestResponse(RestStatus.REQUEST_TIMEOUT, response.toString());
                }
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), EMPTY_PARAMS));
            }
        };
    }

    @Override
    public List<Route> routes() {
        List<Route> routes = new ArrayList<>();
        for (String path : urlPaths) {
            routes.add(new Route(RestRequest.Method.POST, path));
            routes.add(new Route(RestRequest.Method.GET, path));
        }
        return routes;
    }
}
