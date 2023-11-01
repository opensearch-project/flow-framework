package org.opensearch.flowframework.rest;

import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.mockito.Mockito.mock;

public class RestSearchWorkflowActionTests extends OpenSearchTestCase {
    private RestSearchWorkflowAction restSearchWorkflowAction;
    private String searchPath;
    private String invalidTemplate;
    private NodeClient nodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Map.of("baz", "qux"));
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        List<WorkflowNode> nodes = List.of(nodeA, nodeB);
        List<WorkflowEdge> edges = List.of(edgeAB);
        Workflow workflow = new Workflow(Map.of("key", "value"), nodes, edges);
        restSearchWorkflowAction = new RestSearchWorkflowAction();
        Template template = new Template(
                "test",
                "description",
                "use case",
                templateVersion,
                compatibilityVersions,
                Map.of("workflow", workflow),
                TestHelpers.randomUser()
        );
        // Invalid template configuration, wrong field name
        this.invalidTemplate = template.toJson().replace("use_case", "invalid");
        this.searchPath = String.format(Locale.ROOT, "%s/%s", WORKFLOW_URI, "_search");
        this.nodeClient = mock(NodeClient.class);
    }

    public void testConstructor() {
        RestSearchWorkflowAction searchWorkflowAction = new RestSearchWorkflowAction();
        assertNotNull(searchWorkflowAction);
    }

    public void testRestSearchWorkflowActionName() {
        String name = restSearchWorkflowAction.getName();
        assertEquals("search_workflow_action", name);
    }

    public void testRestSearchWorkflowActionRoutes() {
        List<Route> routes = restSearchWorkflowAction.routes();
        assertNotNull(routes);
        assertEquals(2, routes.size());
        assertEquals(RestRequest.Method.POST, routes.get(0).getMethod());
        assertEquals(RestRequest.Method.GET, routes.get(1).getMethod());
        assertEquals(this.searchPath, routes.get(0).getPath());
        assertEquals(this.searchPath, routes.get(1).getPath());
    }

    public void testInvalidSearchRequest() throws IOException {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath(this.searchPath)
                .withContent(new BytesArray(invalidTemplate), MediaTypeRegistry.JSON)
                .build();

        IOException ex = expectThrows(IOException.class, () -> { restSearchWorkflowAction.prepareRequest(request, nodeClient); });
        assertEquals("Unable to parse field [invalid] in a template object.", ex.getMessage());
    }
}
