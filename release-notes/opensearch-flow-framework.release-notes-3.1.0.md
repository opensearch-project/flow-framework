## Version 3.1.0 Release Notes

Compatible with OpenSearch 3.1.0

### Enhancements
- Make thread pool sizes configurable ([#1139](https://github.com/opensearch-project/flow-framework/issues/1139))

### Bug Fixes
- Fixing llm field processing in RegisterAgentStep ([#1151](https://github.com/opensearch-project/flow-framework/pull/1151))
- Include exception type in WorkflowState error field even if no cause ([#1154](https://github.com/opensearch-project/flow-framework/pull/1154))
- Pass llm spec params to builder ([#1155](https://github.com/opensearch-project/flow-framework/pull/1155))

### Infrastructure
- Conditionally include ddb-client dependency only if env variable set ([#1141](https://github.com/opensearch-project/flow-framework/issues/1141))

### Documentation
- Feat: add data summary with log pattern agent template ([#1137](https://github.com/opensearch-project/flow-framework/pull/1137))
