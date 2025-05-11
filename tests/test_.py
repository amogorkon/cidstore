"""Core tests to quickly check the basic functionality of the library on every edit."""

import pytest

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--hypothesis-show-statistics"])
