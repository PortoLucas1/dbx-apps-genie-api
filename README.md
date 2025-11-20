# Databricks Genie API Integration Demo

> **Note**: This code is based on the excellent work by [Vivian Xie](https://github.com/vivian-xie-db) and the original [genie_space repository](https://github.com/vivian-xie-db/genie_space/tree/main). We extend our gratitude to Vivian for creating this foundational implementation that demonstrates Databricks Genie API integration.

This repository demonstrates how to integrate Databricks' AI/BI Genie Conversation APIs into custom Databricks Apps applications, allowing users to interact with their structured data using natural language.

## Overview

This app is a modern Dash application featuring an intuitive chat interface powered by Databricks Genie Conversation APIs, built specifically to run as a Databricks App. This integration showcases how to leverage Databricks' platform capabilities to create interactive data applications with minimal infrastructure overhead.

The Databricks Genie Conversation APIs enable you to embed AI/BI Genie capabilities into any application, allowing users to:

- Ask questions about their data in natural language
- Get SQL-powered insights without writing code
- Follow up with contextual questions in a conversation thread
- View generated SQL queries and results in an interactive format

## Key Features

- **Modern Chat Interface**: Clean, responsive design with sidebar navigation and conversation history
- **Powered by Databricks Apps**: Deploy and run directly from your Databricks workspace with built-in security and scaling
- **Zero Infrastructure Management**: Leverage Databricks Apps to handle hosting, scaling, and security
- **Workspace Integration**: Access your data assets and models directly from your Databricks workspace
- **Natural Language Data Queries**: Ask questions about your data in plain English
- **Stateful Conversations**: Maintain context for follow-up questions
- **Interactive SQL Display**: View and toggle generated SQL queries with syntax highlighting
- **Conversation Management**: Start new chats and navigate between conversation history
- **Customizable Welcome Experience**: Edit welcome messages and suggestion prompts
- **Real-time Feedback**: Thumbs up/down buttons for response quality feedback

## Example Use Case

This demo shows how to create an interactive interface that connects to the Genie API, allowing users to:

1. Start a conversation with a question about their supply chain data
2. View generated SQL queries and results in an interactive format
3. Ask follow-up questions that maintain context
4. Navigate between different conversations using the sidebar
5. Provide feedback on response quality

## Deploying to Databricks Apps

### Prerequisites

- Set up your Python environment and the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- Ensure you have access to a Databricks workspace with Genie Spaces enabled

### Local Development Setup

1. **Edit in your IDE**
   - Set up your Python environment and the Databricks CLI

2. **Clone this repo locally**
   ```bash
   git clone https://github.com/PortoLucas1/dbx-apps-genie-api.git
   ```

3. **Sync future edits back to Databricks**
   Remember to edit this path to match your context. I suggest starting with your personal Workspace folder for development, but saving it to a non-personal folder in production.
   ```bash
   databricks sync --watch . /Workspace/Users/your.email@databricks.com/genie-api
   ```

### Deploy to Databricks Apps

1. **Create the app** (first time only):
   This may take a few minutes, we're creating and starting some compute to be used by the app's frontend.
   ```bash
   databricks apps create genie-chat-ui --description "Genie Chat Interface"
   ```

2. **Update the environment variables** in the app.yaml file:
   ```yaml
   command:
   - "python"
   - "app.py"

   env:
   - name: SPACE_ID
     valueFrom: genie-space
   - name: SQL_WAREHOUSE_ID
     valueFrom: sql-warehouse
   ```

3. **Deploy the app**:
   Remember to edit the path here to match where you synced your code to.
   ```bash
   databricks apps deploy genie-chat-ui --source-code-path /Workspace/Users/your.email@databricks.com/genie-api
   ```
   You can leave out the full path for subsequent deploys.

4. **Configure permissions**:
   - Grant the service principal `can_run` permission to the Genie space
   - Grant the service principal `can_use` permission to the SQL warehouse that powers Genie
   - Grant the service principal appropriate privileges to the underlying resources (catalog, schema, tables)

   **Note**: For demo purposes, ALL PRIVILEGES are used, but you can be more restrictive with `USE CATALOG` on catalog, `USE SCHEMA` on schema, and `SELECT` on tables for production environments.

5. **Open your app in the browser**. If it doesn't work, check out the logs.

### Troubleshooting

| Problem | Solution |
|---------|----------|
| Missing package or wrong package version | Add to requirements.txt |
| Permissions issue | Give access `app-{app-id}` to the resource |
| Missing environment variable | Add to the env section of app.yaml |
| Running the wrong command line at startup | Add to the command section of app.yaml |

For additional troubleshooting, navigate to the Genie space monitoring page and check if the query has been sent successfully to the Genie space via the API. Click open the query and check if there is any error or any permission issues.

## Resources

- [Databricks Genie Documentation](https://docs.databricks.com/aws/en/genie)
- [Conversation APIs Documentation](https://docs.databricks.com/api/workspace/genie)
- [Databricks Apps Documentation](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/)
