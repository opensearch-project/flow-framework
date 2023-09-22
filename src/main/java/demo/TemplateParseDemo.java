/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package demo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.flowframework.template.Template;
import org.opensearch.flowframework.template.TemplateParser;
import org.opensearch.flowframework.workflow.Workflow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map.Entry;

/**
 * Demo class exercising {@link TemplateParser}. This will be moved to a unit test.
 */
public class TemplateParseDemo {

    private static final Logger logger = LogManager.getLogger(TemplateParseDemo.class);

    /**
     * Demonstrate parsing a JSON graph.
     *
     * @param args unused
     * @throws IOException on error.
     */
    @SuppressForbidden(reason = "just a demo class that will be deleted")
    public static void main(String[] args) throws IOException {
        String path = "src/test/resources/template/finaltemplate.json";
        String json;
        try {
            json = new String(Files.readAllBytes(PathUtils.get(path)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("Failed to read JSON at path {}", path);
            return;
        }

        Template t = TemplateParser.parseJsonToTemplate(json);

        System.out.println(t.toJson());
        System.out.println(t.toYaml());

        for (Entry<String, Workflow> e : t.workflows().entrySet()) {
            logger.info("Parsing {} workflow.", e.getKey());
            TemplateParser.parseWorkflowToSequence(e.getValue());
        }
    }
}
