#!/usr/bin/env python3
"""
Build and deployment script for py-orchestrate package.
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, check=check, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return result


def clean_build():
    """Clean previous build artifacts."""
    print("🧹 Cleaning previous build artifacts...")
    
    for path in ["build", "dist", "*.egg-info"]:
        run_command(f"rm -rf {path}", check=False)
    
    print("✅ Cleaned build artifacts")


def build_package():
    """Build the package."""
    print("🔨 Building package...")
    
    result = run_command("uv run python -m build")
    if result.returncode == 0:
        print("✅ Package built successfully")
        
        # List built files
        dist_path = Path("dist")
        if dist_path.exists():
            print("\n📦 Built files:")
            for file in dist_path.iterdir():
                print(f"  - {file.name}")
    else:
        print("❌ Package build failed")
        sys.exit(1)


def check_package():
    """Check the package using twine."""
    print("🔍 Checking package...")
    
    result = run_command("uv run twine check dist/*")
    if result.returncode == 0:
        print("✅ Package check passed")
    else:
        print("❌ Package check failed")
        sys.exit(1)


def test_install():
    """Test installation of the built package."""
    print("🧪 Testing package installation...")
    
    # Create a temporary virtual environment for testing
    run_command("python3 -m venv test_env", check=False)
    
    try:
        # Install the package
        wheel_file = list(Path("dist").glob("*.whl"))[0]
        result = run_command(f"test_env/bin/pip install {wheel_file}")
        
        if result.returncode == 0:
            # Test import
            test_result = run_command("test_env/bin/python -c 'import py_orchestrate; print(\"Import successful!\")'")
            if test_result.returncode == 0:
                print("✅ Package installation and import test passed")
            else:
                print("❌ Package import test failed")
                sys.exit(1)
        else:
            print("❌ Package installation failed")
            sys.exit(1)
    
    finally:
        # Clean up test environment
        run_command("rm -rf test_env", check=False)


def upload_to_testpypi():
    """Upload to Test PyPI."""
    print("🚀 Uploading to Test PyPI...")
    print("Note: You'll need to have your Test PyPI credentials configured")
    
    result = run_command("uv run twine upload --repository testpypi dist/*", check=False)
    if result.returncode == 0:
        print("✅ Successfully uploaded to Test PyPI")
        print("🔗 Check your package at: https://test.pypi.org/project/py-orchestrate/")
    else:
        print("❌ Upload to Test PyPI failed")


def upload_to_pypi():
    """Upload to PyPI."""
    print("🚀 Uploading to PyPI...")
    print("⚠️  WARNING: This will upload to the real PyPI!")
    
    confirm = input("Are you sure you want to upload to PyPI? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Upload cancelled")
        return
    
    result = run_command("uv run twine upload dist/*", check=False)
    if result.returncode == 0:
        print("✅ Successfully uploaded to PyPI")
        print("🔗 Check your package at: https://pypi.org/project/py-orchestrate/")
    else:
        print("❌ Upload to PyPI failed")


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 deploy.py build       - Build the package")
        print("  python3 deploy.py check       - Check the package")
        print("  python3 deploy.py test        - Test installation")
        print("  python3 deploy.py testpypi    - Upload to Test PyPI")
        print("  python3 deploy.py pypi        - Upload to PyPI")
        print("  python3 deploy.py all         - Clean, build, check, and test")
        return
    
    command = sys.argv[1]
    
    if command == "build":
        clean_build()
        build_package()
    elif command == "check":
        check_package()
    elif command == "test":
        test_install()
    elif command == "testpypi":
        upload_to_testpypi()
    elif command == "pypi":
        upload_to_pypi()
    elif command == "all":
        clean_build()
        build_package()
        check_package()
        test_install()
        print("\n🎉 All checks passed! Package is ready for upload.")
        print("Next steps:")
        print("  python3 deploy.py testpypi    # Upload to Test PyPI first")
        print("  python3 deploy.py pypi        # Upload to PyPI when ready")
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()