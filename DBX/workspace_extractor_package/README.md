# Workspace Estimator (WE)

Workspace Estimator allows you to analyze information about your workspace without giving direct access to your system.

## Disclaimer

This application is **not** part of the Snowflake Service and is governed by the terms in LICENSE, unless expressly agreed to in writing. You use this application at your own risk, and Snowflake has no obligation to support your use of this application.

## License

These scripts are licensed under the [Apache license](./LICENSE).

## Requirement

- Account with admin privilege in the workspace.

## How to

Please follow instructions in the [step by step guide](../Step%20By%20Step%20Guide.pdf).

# Building and Publishing to PyPI

This guide explains how to build your Python package using Hatch and publish it to PyPI using twine.

## Prerequisites

Ensure you have the required tools installed:

```bash
pip install hatch twine
```

## Step 1: Build the Package

### Using Hatch (recommended)
```bash
# Clean any previous builds
rm -rf dist/ build/ *.egg-info/

# Build the package using Hatch
hatch build
```

After building, you should see the generated files in the `dist/` directory:
- `workspace_extractor-x.y.z.tar.gz` (source distribution)
- `workspace_extractor-x.y.z-py3-none-any.whl` (wheel distribution)

## Step 2: Test Your Build

Before uploading to PyPI, test your package locally:

```bash
# Install your package locally
pip install dist/your-package-name-version.tar.gz

# Or install from wheel
pip install dist/your-package-name-version-py3-none-any.whl
```

## Step 3: Upload to PyPI

### Option A: Upload to Test PyPI (Recommended First)

1. Upload to Test PyPI:

```bash
twine upload --repository testpypi dist/*
```

4. Test installation from Test PyPI:
```bash
pip install --index-url https://test.pypi.org/simple/ your-package-name
```

### Option B: Upload to Production PyPI

1. Upload to PyPI:

```bash
twine upload dist/*
```

## Step 4: Authentication

### Using API Tokens (Recommended)

After the ``twine upload dist/*`` command is executed it will ask for a password (API Token)

## Complete Workflow Example

```bash
# 1. Clean previous builds
rm -rf dist/ build/ *.egg-info/

# 2. Build the package using Hatch
hatch build

# 3. Check the build
twine check dist/*

# 4. Upload to Test PyPI first
twine upload --repository testpypi dist/*

# 5. Test installation
pip install --index-url https://test.pypi.org/simple/ your-package-name

# 6. If everything works, upload to production PyPI
twine upload dist/*
```

## Troubleshooting

### Common Issues

- **"Package already exists"**: You cannot overwrite existing versions. Increment your version number in `pyproject.toml` or `setup.py`.
- **Authentication failed**: Check your API token or credentials.
- **File not found**: Ensure you've built the package first with `hatch build`.

### Checking Your Build

```bash
# Verify your package contents
twine check dist/*

# List contents of your wheel
unzip -l dist/your-package-name-version-py3-none-any.whl
```

### Version Management

Always increment your version number for new releases in `__version__.py`:
- Patch version: `1.0.0` → `1.0.1` (bug fixes)
- Minor version: `1.0.0` → `1.1.0` (new features)
- Major version: `1.0.0` → `2.0.0` (breaking changes)

Note: With Hatch, the version is managed in the `__version__.py` file as configured in `pyproject.toml`.

## Security Best Practices

1. **Use API tokens** instead of username/password
2. **Limit token scope** to specific projects when possible
3. **Keep tokens secure** - don't commit them to version control
4. **Use Test PyPI first** to avoid mistakes on production