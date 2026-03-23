-- ============================================
-- Snowflake External Access Integration for GitHub API
-- ============================================
--
-- This script provides a template for setting up external access
-- to the GitHub API from Snowflake notebooks and stored procedures.
--
-- IMPORTANT: This is a TEMPLATE only. You must replace all placeholder
-- values with your actual configuration before executing.
--
-- SECURITY NOTES:
-- - Only allow access to specific GitHub API endpoints, not all of github.com
-- - Scope the integration to specific roles that need access
-- - Never hardcode API tokens or secrets in SQL files
-- - Use Snowflake secrets or external secrets managers for credentials
-- - Use the minimum required permissions in GitHub (read-only when possible)
--
-- ============================================

-- ============================================================
-- STEP 1: Create a Network Rule for GitHub API Access
-- ============================================================
--
-- Network rules define which external endpoints can be accessed.
-- We allow specific GitHub API endpoints, not the entire domain.
--
-- Replace placeholders:
--   - <NETWORK_RULE_NAME>: Unique name for your network rule
--   - <GITHUB_API_HOST>: GitHub API hostname (e.g., "api.github.com")
--

-- Create network rule for GitHub API
CREATE OR REPLACE NETWORK RULE <NETWORK_RULE_NAME>
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('<GITHUB_API_HOST>:443')
  COMMENT = 'Allow outbound HTTPS access to GitHub API for notebook integration';

-- Example with specific path scoping (if using proxy or API gateway):
-- CREATE OR REPLACE NETWORK RULE <NETWORK_RULE_NAME>
--   MODE = EGRESS
--   TYPE = HOST_PORT
--   VALUE_LIST = ('api.github.com:443')
--   COMMENT = 'Allow access to GitHub API endpoints';

-- ============================================================
-- STEP 2: Create External Access Integration
-- ============================================================
--
-- External access integrations bundle network rules with optional secrets.
-- This integration will be used by stored procedures and notebooks.
--
-- Replace placeholders:
--   - <INTEGRATION_NAME>: Unique name for your integration
--   - <NETWORK_RULE_NAME>: Must match the name from Step 1
--
-- Note: This example does NOT include secrets. For authenticated
-- GitHub API calls, you should:
--   1. Store API tokens in Snowflake secrets or external vault
--   2. Add SECRET entries to this integration
--   3. Reference the secret in your stored procedure code
--

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION <INTEGRATION_NAME>
  ALLOWED_NETWORK_RULES = (<NETWORK_RULE_NAME>)
  ENABLED = TRUE
  COMMENT = 'External access integration for GitHub API calls from Snowflake notebooks';

-- Example with secret reference (for authenticated requests):
-- CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION <INTEGRATION_NAME>
--   ALLOWED_NETWORK_RULES = (<NETWORK_RULE_NAME>)
--   ALLOWED_AUTHENTICATION_SECRETS = (<SECRET_NAME>)
--   ENABLED = TRUE
--   COMMENT = 'GitHub API integration with secret reference';

-- ============================================================
-- STEP 3: Grant Usage on Integration
-- ============================================================
--
-- Grant USAGE on the external access integration to roles that need
-- to use GitHub API from stored procedures or notebooks.
--
-- IMPORTANT: Follow principle of least privilege. Only grant to
-- roles that actually need to access external APIs.
--
-- Replace placeholders:
--   - <ROLE_NAME>: The role that needs to use this integration
--

GRANT USAGE ON INTEGRATION <INTEGRATION_NAME> TO ROLE <ROLE_NAME>;

-- ============================================================
-- STEP 4: Example Stored Procedure Using External Access
-- ============================================================
--
-- This example shows how to use the external access integration
-- in a Snowflake stored procedure to call the GitHub API.
--
-- Replace placeholders:
--   - <PROCEDURE_NAME>: Name for your stored procedure
--   - <INTEGRATION_NAME>: Must match the name from Step 2
--   - <GITHUB_API_PATH>: The API endpoint path (e.g., "/repos/owner/repo/issues")
--

CREATE OR REPLACE PROCEDURE <PROCEDURE_NAME>(<PARAMETERS>)
  RETURNS <RETURN_TYPE>
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  HANDLER = 'main'
  EXTERNAL_ACCESS_INTEGRATIONS = (<INTEGRATION_NAME>)
  PACKAGES = ('snowflake-snowpark-python', 'requests')
  AS $$
  import requests
  import sys
  import logging

  # Configure logging
  logger = logging.getLogger(__name__)
  logger.addHandler(logging.StreamHandler(sys.stdout))
  logger.setLevel(logging.INFO)

  def main(<PARAMETERS>):
    """
    Call GitHub API using Snowflake external access integration.

    Args:
      <PARAMETER_DESCRIPTIONS>

    Returns:
      <RETURN_DESCRIPTION>
    """
    # GitHub API endpoint
    github_api_url = "https://<GITHUB_API_HOST>/<GITHUB_API_PATH>"

    # Example: Unauthenticated request (public endpoints only)
    # response = requests.get(github_api_url)

    # Example: Authenticated request using Snowflake secret
    # This requires:
    #   1. Create a secret containing your GitHub API token
    #   2. Add the secret to the integration definition
    #   3. Retrieve the secret value in this procedure
    # token = _get_secret_value('<SECRET_NAME>')
    # headers = {"Authorization": f"Bearer {token}"}
    # response = requests.get(github_api_url, headers=headers)

    # Check response status
    if response.status_code != 200:
      logger.error(f"GitHub API request failed: {response.status_code}")
      raise Exception(f"GitHub API error: {response.status_code}")

    # Return parsed JSON response
    return response.json()

  def _get_secret_value(secret_name):
    """
    Retrieve a secret value by name from Snowflake secrets.

    Args:
      secret_name: Name of the secret in Snowflake

    Returns:
      The secret value
    """
    import snowflake.snowpark as snowpark

    # Get the session object
    session = snowpark.Session.get_active_session()

    # Query the secret
    # Note: This is a placeholder - actual secret retrieval depends on
    # your Snowflake secrets management setup
    secret_query = f"SELECT PARSE_JSON(secret_value):value FROM secrets WHERE name = '{secret_name}'"
    result = session.sql(secret_query).collect()

    if not result:
      raise ValueError(f"Secret not found: {secret_name}")

    return result[0][0]
  $$;

-- ============================================================
-- EXAMPLE: Fetch Repository Issues from GitHub
-- ============================================================
--
-- This is a complete example of a stored procedure that fetches
-- issues from a GitHub repository using the external access integration.
--

-- Example procedure signature (adjust parameters as needed):
CREATE OR REPLACE PROCEDURE fetch_github_issues(
    repo_owner VARCHAR,
    repo_name VARCHAR,
    state VARCHAR DEFAULT 'open'
  )
  RETURNS TABLE (
    issue_id NUMBER,
    title VARCHAR,
    state VARCHAR,
    created_at TIMESTAMP_LTZ,
    html_url VARCHAR
  )
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  HANDLER = 'fetch_issues'
  EXTERNAL_ACCESS_INTEGRATIONS = (<INTEGRATION_NAME>)
  PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
  AS $$
  import requests
  import pandas as pd
  import snowflake.snowpark as snowpark
  from datetime import datetime

  def fetch_issues(repo_owner, repo_name, state):
    """
    Fetch issues from a GitHub repository.

    Args:
      repo_owner: GitHub repository owner (e.g., "octocat")
      repo_name: GitHub repository name (e.g., "Hello-World")
      state: Issue state ('open', 'closed', 'all')

    Returns:
      DataFrame with issue data
    """
    # Construct GitHub API URL
    github_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/issues"

    # Make API request (public endpoint, no authentication required)
    response = requests.get(
      github_url,
      params={'state': state, 'per_page': 100},
      headers={'Accept': 'application/vnd.github.v3+json'}
    )

    # Check for errors
    if response.status_code != 200:
      raise Exception(f"GitHub API error: {response.status_code} - {response.text}")

    # Parse JSON response
    issues_data = response.json()

    # Convert to DataFrame
    df = pd.DataFrame(issues_data)

    # Select and transform columns
    result_df = pd.DataFrame({
      'issue_id': df['id'],
      'title': df['title'],
      'state': df['state'],
      'created_at': pd.to_datetime(df['created_at']),
      'html_url': df['html_url']
    })

    return result_df
  $$;

-- Example usage:
-- SELECT * FROM TABLE(fetch_github_issues('octocat', 'Hello-World', 'open'));

-- ============================================================
-- VERIFICATION QUERIES
-- ============================================================
--
-- Run these queries to verify the setup:
--

-- Show network rule details
-- SELECT * FROM INFORMATION_SCHEMA.NETWORK_RULES WHERE RULE_NAME = '<NETWORK_RULE_NAME>';

-- Show external access integration details
-- SELECT * FROM INFORMATION_SCHEMA.EXTERNAL_ACCESS_INTEGRATIONS
--   WHERE INTEGRATION_NAME = '<INTEGRATION_NAME>';

-- Show roles with access to the integration
-- SELECT * FROM INFORMATION_SCHEMA.GRANTS_TO_ROLES
--   WHERE GRANTED_ON = 'INTEGRATION'
--   AND NAME = '<INTEGRATION_NAME>';

-- ============================================================
-- CLEANUP (USE WITH CAUTION)
-- ============================================================
--
-- To remove the external access integration and network rule:
--
-- DROP PROCEDURE IF EXISTS <PROCEDURE_NAME>;
-- DROP EXTERNAL ACCESS INTEGRATION IF EXISTS <INTEGRATION_NAME>;
-- DROP NETWORK RULE IF EXISTS <NETWORK_RULE_NAME>;
-- REVOKE USAGE ON INTEGRATION <INTEGRATION_NAME> FROM ROLE <ROLE_NAME>;
-- ============================================================

-- ============================================
-- END OF TEMPLATE
-- ============================================
