# py-utils

A collection of Python utilities for common tasks.

## Installation

### From GitHub

You can install directly from GitHub using pip:

```bash
pip install git+https://github.com/xzsean666/py-utils.git
```

To install a specific version or branch:

```bash
# Install from specific branch
pip install git+https://github.com/xzsean666/py-utils.git@main

# Install from specific tag/release
pip install git+https://github.com/xzsean666/py-utils.git@v0.1.0
```

### From Source

Clone the repository and install in editable mode:

```bash
git clone https://github.com/xzsean666/py-utils.git
cd py-utils
pip install -e .
```

### Install with Development Dependencies

```bash
pip install -e ".[dev]"
```

## Usage

```python
from py_utils.utils import example_function

result = example_function()
print(result)  # Output: Hello from py-utils!
```

## Development

### Setup Development Environment

```bash
git clone https://github.com/xzsean666/py-utils.git
cd py-utils
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest
# With coverage
pytest --cov=py_utils tests/
```

### Code Formatting

```bash
black src/ tests/
```

### Linting

```bash
flake8 src/ tests/
mypy src/
```

## Project Structure

```
py-utils/
├── src/py_utils/          # Main package source code
│   ├── __init__.py        # Package initialization
│   └── utils.py           # Utility functions
├── tests/                 # Test files
├── pyproject.toml         # Project metadata and build configuration
├── README.md              # This file
└── LICENSE                # MIT License
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.
