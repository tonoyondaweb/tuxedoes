# Snowflake Discovery Environment

Quick Summary
> Build a version-controlled Snowflake metadata catalog with automated extraction and three-layer trigger architecture.

## Overview
The Snowflake Discovery system automatically extracts full DDLs, constraints, tags, masking policies, and VARIANT schemas from Snowflake databases. The extracted metadata is saved in a version-controlled repository with both human-readable .sql files and machine-readable .json files.

Three-layer trigger architecture ensures changes are properly reviewed before reaching production:
1. **Branch Creation**: Automatic discovery and commit to new branches
2. **Snowsight Manual**: Manual trigger from Snowflake Notebook after diff detection
3. **Main Gate**: All changes to main branch must go through Pull Request review

## Architecture
See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation including:
- Architecture Decision Records (ADRs)
- Data flow diagrams
- Design tree decisions
- Technology stack

## Quick Start

### Prerequisites
- Python 3.9+
- Snowflake account with key-pair authentication
- GitHub repository for workflow triggers

### Installation
```bash
git clone <your-repo-url>
cd <your-repo>
pip install -e .
```

## Configuration
Configuration is managed via `discovery-config.yml`. See [discovery-config.example.yml](discovery-config.example.yml) for a complete configuration reference.

Key configuration sections:
- Snowflake connection settings (account, user, warehouse, private key)
- GitHub integration (owner, repo, branch, workflow)
- Discovery targets (databases and schemas)
- VARIANT column sampling settings
- Output format options

## Folder Structure

```
discovery/
├── {database}/
│   ├── {schema}/
│   │   ├── tables/
│   │   ├── views/
│   │   ├── procedures/
│   │   ├── functions/
│   │   ├── streams/
│   │   └── tasks/
└── _manifest.json
```

Each object has both `.sql` and `.json` files with cross-references.

## Snowflake Setup

### 1. Create a GitHub App
Create a GitHub App with Contents Read and Write permissions.

### 2. Create API Integration in Snowflake
See [sql/setup_api_integration.sql](sql/setup_api_integration.sql) for setup instructions.

### 3. Create External Access Integration
Create External Access Integration for api.github.com (see [sql/setup_external_access.sql](sql/setup_external_access.sql)).

### 4. Grant Snowflake Roles Access
Grant Snowflake roles access to the integrations.

### 5. Install GitHub App
Install the GitHub App in your organization.

### 6. Configure Secrets
Configure secrets in Snowflake or GitHub Actions.

## GitHub Actions Setup

The GitHub Actions workflow [.github/workflows/discover.yml](.github/workflows/discover.yml) runs on:
- Branch creation (automatic discovery)
- Manual trigger (workflow_dispatch)

### Required Secrets
Configure the following secrets in your repository:
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier
- `SNOWFLAKE_USER`: Your Snowflake username
- `SNOWFLAKE_WAREHOUSE`: Your Snowflake warehouse name
- `SNOWFLAKE_PRIVATE_KEY_RAW`: Your RSA private key (PEM format)
- `SNOWFLAKE_ROLE`: Your Snowflake role name
- `GITHUB_TOKEN`: Automatically provided by GitHub Actions

## CLI Usage

```bash
# Extract metadata
python -m discovery extract --config discovery-config.yml

# Validate configuration
python -m discovery validate-config discovery-config.yml

# Run extraction in dry-run mode (no file writes)
python -m discovery extract --config discovery-config.yml --dry-run
```

## Contributing
Contributions are welcome! Please:
1. Fork the repository and create a feature branch
2. Make your changes with clear commit messages
3. Ensure all tests pass: `pytest tests/`
4. Follow the existing code style and conventions
5. Update documentation as needed
6. Submit a Pull Request with a clear description of changes

## License
MIT License

Copyright (c) 2026

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
