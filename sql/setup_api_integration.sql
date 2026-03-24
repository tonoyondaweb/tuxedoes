-- ================================================================
-- Snowflake Git API Integration Setup
-- Purpose: Enable Snowflake Notebooks to access GitHub repository
-- Requires: GitHub App with proper permissions
-- ================================================================

-- Create API Integration for GitHub Git API
CREATE OR REPLACE API INTEGRATION <INTEGRATION_NAME>
  API_PROVIDER = 'git_https_api'
  API_ALLOWED_PREFIXES = ('https://github.com/')
  API_USER_AUTHENTICATION = (TYPE = snowflake_github_app)
  ENABLED = TRUE
  COMMENT = 'Git API integration for Snowflake Notebooks to access GitHub repository'
;

-- Create Git Repository to sync notebooks
-- Note: This references the API integration created above
CREATE OR REPLACE GIT REPOSITORY <REPO_NAME>
  API_INTEGRATION = '<INTEGRATION_NAME>'
  ORGANIZATION = '<GITHUB_ORG>'
  REPOSITORY = '<GITHUB_REPO>'
  BRANCH = 'main'  -- Default branch
  COMMENT = 'Git repository for Snowflake Notebooks sync'
;

-- ================================================================
-- Setup Instructions
-- ================================================================
--
-- 1. Create a GitHub App with permissions:
--    - Contents: Read (for reading files)
--    - Contents: Write (for creating PRs, if needed)
--    - Repository administration: Read (for accessing repo metadata)
--
-- 2. Install the GitHub App in your GitHub organization
--
-- 3. Grant Snowflake service account access to use this integration
--
-- 4. Run the above DDL statements in Snowflake
--
-- 5. Replace <INTEGRATION_NAME> with your preferred integration name
--
-- 6. Replace <REPO_NAME> with your preferred repository name
--
-- 7. Replace <GITHUB_ORG> and <GITHUB_REPO> with your GitHub org/repo
--
-- ================================================================
