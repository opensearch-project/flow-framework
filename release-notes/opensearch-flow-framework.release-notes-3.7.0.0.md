## Version 3.7.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.7.0

### Features

* Add missing fields to workflow steps matching ml-commons builders, enabling MCP connector creation and unified agent interface ([#1360](https://github.com/opensearch-project/flow-framework/pull/1360))
* Add response processor to flow agent agentic search template for DSL query exposure ([#1367](https://github.com/opensearch-project/flow-framework/pull/1367))

### Enhancements

* Validate name and description fields in ML Commons steps during workflow parsing using ML Commons validation methods ([#1368](https://github.com/opensearch-project/flow-framework/pull/1368))
* Set `provisioned_by` field on ML resources to enable adoption metrics attribution ([#1388](https://github.com/opensearch-project/flow-framework/pull/1388))

### Bug Fixes

* Handle ResourceAlreadyExistsException in FlowFrameworkIndicesHandler to fix race condition on multi-node clusters ([#1378](https://github.com/opensearch-project/flow-framework/pull/1378))

### Infrastructure

* Add Gradle cache to setup-java steps in CI workflows to reduce build times ([#1372](https://github.com/opensearch-project/flow-framework/pull/1372))

### Maintenance

* Support Jackson 3.x release line ([#1376](https://github.com/opensearch-project/flow-framework/pull/1376))
