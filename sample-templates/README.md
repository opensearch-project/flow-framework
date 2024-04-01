## Flow Framework Sample Templates

This folder contains sample workflow templates that can be used with Flow Framework.

Each template is provided in both YAML and JSON format with identical functionality.
The YAML templates include comments which give more insight into the template's usage.
Use the corresponding `Content-Type` (`application/yaml` or `application/json`) when providing them as the body of a REST request.

Note that several of the templates use both the single quote (`'`) and double quote (`"`) characters which may create issues if using the templates using `curl` on the command line with `--data`. Escaping the single quotes or reading the template from a file is needed to work around this.

You will need to update the `credentials` field with appropriate API keys.

To create a workflow and provision the resources:

```
POST /_plugins/_flow_framework/workflow?provision=true
{ template as body }
```

This will return a `workflow_id`. To get the IDs of created resources, call the workflow status API.

```
GET /_plugins/_flow_framework/workflow/<workflow_id>/_status
```

For the Query Assist Agent API, the `agent_id` of the `root_agent` can be used to query it.

```
POST /_plugins/_ml/agents/_<agent_id>/_execute
{
  "parameters": {
    "question": "How many 5xx logs do I have?"
  }
}
```