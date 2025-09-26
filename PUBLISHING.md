# Publishing to PyPI

## Setup (One-time)

1. **Create PyPI API tokens:**
   - Go to [PyPI Account Settings](https://pypi.org/manage/account/)
   - Create an API token for this project
   - Go to [Test PyPI Account Settings](https://test.pypi.org/manage/account/)
   - Create an API token for testing

2. **Add GitHub Secrets:**
   - Go to your GitHub repository → Settings → Secrets and variables → Actions
   - Add these secrets:
     - `PYPI_API_TOKEN`: Your PyPI API token
     - `TEST_PYPI_API_TOKEN`: Your Test PyPI API token

## Publishing a Release

1. **For a pre-release (e.g., `1.0.0-beta1`):**
   ```bash
   git tag 1.0.0-beta1
   git push origin 1.0.0-beta1
   ```
   This will publish to Test PyPI.

2. **For a stable release (e.g., `1.0.0`):**
   ```bash
   git tag 1.0.0
   git push origin 1.0.0
   ```
   This will publish to PyPI.

3. **The GitHub Action will automatically:**
   - Update the version in `pyproject.toml`
   - Build the package
   - Run quality checks
   - Publish to PyPI/Test PyPI
   - Create a GitHub release with artifacts
