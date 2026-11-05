## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Enhancements

* Deprecate `filter_by_backend_roles` setting, targeting removal in 4.0 in favor of the security plugin's resource sharing and access control ([#1490](https://github.com/opensearch-project/flow-framework/pull/1490))

### Bug Fixes

* Fix missing JSON-B provider by moving Yasson dependency to the correct `org.eclipse.yasson` group ID ([#1479](https://github.com/opensearch-project/flow-framework/pull/1479))
