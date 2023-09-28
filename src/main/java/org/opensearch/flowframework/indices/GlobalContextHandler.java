package org.opensearch.flowframework.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.workflow.CreateIndexStep;
import org.opensearch.flowframework.workflow.WorkflowData;

import java.io.IOException;
import java.util.List;

import static org.opensearch.flowframework.constant.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.constant.CommonValue.GLOBAL_CONTEXT_INDEX_MAPPING;
import static org.opensearch.flowframework.workflow.CreateIndexStep.getIndexMappings;

public class GlobalContextHandler {
    private static final Logger logger = LogManager.getLogger(GlobalContextHandler.class);
    private CreateIndexStep createIndexStep;

    public GlobalContextHandler(CreateIndexStep createIndexStep) {
        this.createIndexStep = createIndexStep;
    }

    /**
     * Get global-context index mapping
     * @return global-context index mapping
     * @throws IOException if mapping file cannot be read correctly
     */
    public static String getGlobalContextMappings() throws IOException {
        return getIndexMappings(GLOBAL_CONTEXT_INDEX_MAPPING);
    }

    private void initGlobalContextIndexIfAbsent(ActionListener<Boolean> listener) {
        createIndexStep.initIndexIfAbsent(FlowFrameworkIndex.GLOBAL_CONTEXT, listener);
    }

    public void putGlobalContextDocument(ActionListener<IndexResponse> listener) {
        initGlobalContextIndexIfAbsent(listener.wrap(indexCreated -> {
            if (!indexCreated) {

            }
            IndexRequest request = new IndexRequest(GLOBAL_CONTEXT_INDEX);
            try () {

            } catch (Exception e) {

            }
        }, e -> {

        }));
    }

    public void storeResponseToGlobalContext(String documentId, List<WorkflowData> workflowDataList) {

    }
}
