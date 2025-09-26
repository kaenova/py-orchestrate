#!/usr/bin/env bash
set -e

# Release helper script for py-orchestrate
# This script helps test releases locally before pushing tags

VERSION="$1"

if [ -z "$VERSION" ]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 1.0.0"
    echo "Example: $0 1.0.0-beta1"
    exit 1
fi

# Validate version format
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-.*)?$ ]]; then
    echo "❌ Invalid version format. Use semantic versioning (e.g., 1.0.0 or 1.0.0-beta1)"
    exit 1
fi

echo "🏷️  Preparing release: $VERSION"

# Check if working directory is clean
if [ -n "$(git status --porcelain)" ]; then
    echo "❌ Working directory is not clean. Please commit or stash changes."
    exit 1
fi

# Update version in pyproject.toml
echo "📝 Updating version in pyproject.toml..."
sed -i.bak "s/version = \".*\"/version = \"$VERSION\"/" pyproject.toml
rm pyproject.toml.bak

# Show the change
echo "Updated version:"
grep 'version = ' pyproject.toml

# Build and test
echo "🔨 Building package..."
uv run python -m build

echo "🔍 Checking package..."
uv run twine check dist/*

echo "🧪 Testing import..."
# Create temporary venv and test installation
python3 -m venv test_env
test_env/bin/pip install dist/*.whl
test_env/bin/python -c "import py_orchestrate; print('✅ Import test passed')"
rm -rf test_env

echo "✅ All checks passed!"
echo ""
echo "Next steps:"
echo "1. Review the changes:"
echo "   git diff"
echo ""
echo "2. Commit the version update:"
echo "   git add pyproject.toml"
echo "   git commit -m \"Bump version to $VERSION\""
echo ""
echo "3. Create and push the tag:"
echo "   git tag $VERSION"
echo "   git push origin main"
echo "   git push origin $VERSION"
echo ""
echo "4. The GitHub Action will automatically:"
if [[ "$VERSION" == *"-"* ]]; then
    echo "   - Build and publish to Test PyPI (pre-release)"
else
    echo "   - Build and publish to PyPI (stable release)"
fi
echo "   - Create a GitHub release"
echo ""
echo "🚀 Ready for release!"