# py-orchestrate PyPI Release Setup

## Complete Setup Guide

This document provides a complete guide for setting up automated PyPI releases using GitHub Actions.

## 🏗️ Repository Structure

```
py-orchestrate/
├── .github/
│   ├── workflows/
│   │   ├── ci.yml              # CI for PRs and pushes
│   │   └── publish.yml         # PyPI publishing on tags
│   ├── ISSUE_TEMPLATE/
│   │   ├── bug_report.md
│   │   └── feature_request.md
│   └── pull_request_template.md
├── py_orchestrate/             # Main package
│   ├── __init__.py
│   ├── decorators.py
│   ├── models.py
│   ├── orchestrator.py
│   └── example.py
├── .gitignore
├── CHANGELOG.md
├── LICENSE
├── MANIFEST.in
├── README.md
├── deploy.py                   # Local deployment script
├── pyproject.toml              # Package configuration
└── release.sh                  # Release preparation script
```

## 🔐 GitHub Secrets Setup

### Required Secrets

Go to your GitHub repository → Settings → Secrets and variables → Actions

Add these repository secrets:

1. **`PYPI_API_TOKEN`**
   - Go to https://pypi.org/manage/account/
   - Create API token for this project
   - Copy the token (starts with `pypi-`)

2. **`TEST_PYPI_API_TOKEN`**
   - Go to https://test.pypi.org/manage/account/
   - Create API token for this project
   - Copy the token (starts with `pypi-`)

## 🚀 Release Process

### 1. Pre-release Testing (Optional)

```bash
# Test with a pre-release version
./release.sh 0.1.0-beta1
git add pyproject.toml
git commit -m "Bump version to 0.1.0-beta1"
git tag 0.1.0-beta1
git push origin main
git push origin 0.1.0-beta1
```

This will:
- ✅ Publish to Test PyPI
- ✅ Create a pre-release on GitHub
- ✅ Allow testing before stable release

### 2. Stable Release

```bash
# Prepare stable release
./release.sh 0.1.0
git add pyproject.toml
git commit -m "Bump version to 0.1.0"
git tag 0.1.0
git push origin main
git push origin 0.1.0
```

This will:
- ✅ Publish to PyPI
- ✅ Create a stable release on GitHub
- ✅ Generate release notes automatically

## 🤖 What GitHub Actions Do

### CI Workflow (`.github/workflows/ci.yml`)
**Triggers:** Push to main/develop, Pull Requests
- ✅ Tests on Ubuntu, Windows, macOS
- ✅ Tests Python 3.12 and 3.13
- ✅ Import tests
- ✅ Type checking with mypy
- ✅ Code formatting with black
- ✅ Package build test

### Publish Workflow (`.github/workflows/publish.yml`)
**Triggers:** Git tags matching `x.x.x` or `x.x.x-*`
- ✅ Updates version in pyproject.toml
- ✅ Builds wheel and source distribution  
- ✅ Runs quality checks
- ✅ Publishes to Test PyPI (pre-release) or PyPI (stable)
- ✅ Creates GitHub release with artifacts

## 📦 Package Features

- **Automated Versioning**: Version automatically updated from git tags
- **Quality Checks**: Twine validation, type checking, formatting
- **Multi-platform**: Tested on Linux, Windows, macOS
- **Artifact Storage**: Built packages attached to GitHub releases
- **Pre-release Support**: Beta/alpha releases go to Test PyPI first

## 🔧 Local Development

```bash
# Install dev dependencies
uv sync --dev

# Run tests
uv run python py_orchestrate/example.py

# Type check
uv run mypy py_orchestrate --ignore-missing-imports

# Format code
uv run black py_orchestrate

# Build locally
uv run python -m build

# Test local build
./release.sh 0.1.0-test
```

## 📝 Version Management

### Semantic Versioning
- **Major** (1.0.0): Breaking changes
- **Minor** (0.1.0): New features, backward compatible
- **Patch** (0.0.1): Bug fixes, backward compatible

### Pre-release Versions
- **Alpha**: 1.0.0-alpha1 (early testing)
- **Beta**: 1.0.0-beta1 (feature complete, testing)
- **RC**: 1.0.0-rc1 (release candidate)

## 🎯 First Release Checklist

- [ ] Update author information in pyproject.toml
- [ ] Update GitHub URLs in pyproject.toml
- [ ] Set up PyPI and Test PyPI accounts
- [ ] Create API tokens for both PyPI services
- [ ] Add secrets to GitHub repository
- [ ] Test CI workflow with a PR
- [ ] Create first pre-release tag: `git tag 0.1.0-beta1`
- [ ] Verify Test PyPI publication
- [ ] Create stable release tag: `git tag 0.1.0`
- [ ] Verify PyPI publication

## 🐛 Troubleshooting

### Common Issues

1. **"Package already exists"**
   - You can't re-upload the same version
   - Increment version number

2. **"Invalid credentials"**
   - Check GitHub secrets are set correctly
   - Verify API tokens are not expired

3. **"Build failed"**
   - Check the GitHub Actions logs
   - Test build locally first

4. **"Import error"**
   - Ensure all required files are in MANIFEST.in
   - Check package structure

### Getting Help

- Check GitHub Actions logs for detailed error messages
- Test locally with `./release.sh` before pushing tags
- Use Test PyPI for testing before stable releases
- Open issues for any problems

## 🎉 Success!

Once set up, your release process is as simple as:

```bash
git tag 1.0.0
git push origin 1.0.0
```

And GitHub Actions handles the rest! 🚀