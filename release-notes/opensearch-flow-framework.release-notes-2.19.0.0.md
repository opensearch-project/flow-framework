## Version 2.19.0.0

Compatible with OpenSearch 2.19.0

### Features
- Add multitenant remote metadata client ([#980](https://github.com/opensearch-project/flow-framework/pull/980))
- Add synchronous execution option to workflow provisioning ([#990](https://github.com/opensearch-project/flow-framework/pull/990))

### Bug Fixes
- Remove useCase and defaultParams field in WorkflowRequest ([#758](https://github.com/opensearch-project/flow-framework/pull/758))
- Fix RBAC fetching from workflow state when template is not present ([#998](https://github.com/opensearch-project/flow-framework/pull/998))

### Refactoring
- Replace String concatenation with Log4j ParameterizedMessage for readability ([#943](https://github.com/opensearch-project/flow-framework/pull/943))
