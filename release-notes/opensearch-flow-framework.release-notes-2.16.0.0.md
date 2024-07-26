## Version 2.16.0.0

Compatible with OpenSearch 2.16.0

### Enhancements
- Register system index descriptors through SystemIndexPlugin.getSystemIndexDescriptors ([#750](https://github.com/opensearch-project/flow-framework/pull/750))
- Support editing of certain workflow fields on a provisioned workflow ([#757](https://github.com/opensearch-project/flow-framework/pull/757))
- Add allow_delete parameter to Deprovision API ([#763](https://github.com/opensearch-project/flow-framework/pull/763))
- Improve Template and WorkflowState builders ([#778](https://github.com/opensearch-project/flow-framework/pull/778))

### Bug Fixes
- Handle Not Found deprovision exceptions as successful deletions ([#805](https://github.com/opensearch-project/flow-framework/pull/805))
- Wrap CreateIndexRequest mappings in _doc key as required ([#809](https://github.com/opensearch-project/flow-framework/pull/809))
- Have FlowFrameworkException status recognized by ExceptionsHelper ([#811](https://github.com/opensearch-project/flow-framework/pull/811))
