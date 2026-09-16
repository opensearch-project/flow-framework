## Version 3.9.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.9.0

### Bug Fixes

* Declare workflow_state as a child resource of workflow so state documents inherit access from their parent workflow ([#1455](https://github.com/opensearch-project/flow-framework/pull/1455))

### Infrastructure

* Always use 1 shard for system indexes ([#1468](https://github.com/opensearch-project/flow-framework/pull/1468))

### Maintenance

* Rename resource sharing feature flag settings to the non-experimental key ([#1482](https://github.com/opensearch-project/flow-framework/pull/1482))
