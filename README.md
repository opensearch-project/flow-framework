## OpenSearch Flow Framework

This project is an OpenSearch plugin that enables builders to innovate AI applications on OpenSearch.

The current process of using ML offerings in OpenSearch, such as Semantic Search, requires users to handle complex setup and pre-processing tasks, and send verbose user queries, both of which can be time-consuming and error-prone.

We want to introduce our customers to a new no-code/low-code builder experience ([Backend RFC](https://github.com/opensearch-project/OpenSearch/issues/9213) and [Frontend RFC](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/4755)) that empowers users to compose AI-augmented query and ingestion flows, integrate ML models supported by ML-Commons, and streamline the OpenSearch app development experience through a drag-and-drop designer.  The front end will help users create use case templates, which provide a compact description of configuration steps for automated workflows such as Retrieval Augment Generation (RAG), AI connectors and other components that prime OpenSearch as a backend to leverage generative models.  Once primed, builders can query OpenSearch directly without building middleware logic to stitch together data flows and ML models.

See the [Development Plan](https://github.com/opensearch-project/flow-framework/issues/475) to view or comment on current incremental development priorities.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
