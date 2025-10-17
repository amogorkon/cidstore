#!/bin/bash
# Enable Python 3.13 optimizations for CIDStore
# Run this script to set environment variables for your session:
#   source ./scripts/enable_optimizations.sh
# Or add to your .bashrc/.zshrc for permanent configuration

echo "Enabling Python 3.13 optimizations..."
echo ""

# Check if Python supports free-threading
supportsGIL=$(python -c "import sys; print(hasattr(sys, '_is_gil_enabled'))" 2>/dev/null)

if [ "$supportsGIL" = "True" ]; then
    # Enable free-threading (GIL-free mode)
    export PYTHON_GIL=0
    echo "✓ Free-threading enabled (PYTHON_GIL=0)"
else
    echo "⚠ Free-threading NOT available"
    echo "  Your Python build doesn't support --disable-gil"
    echo "  To enable: rebuild Python with ./configure --disable-gil"
fi

# Enable JIT compiler
export PYTHON_JIT=1
echo "✓ JIT compiler enabled (PYTHON_JIT=1)"

# Enable auto-optimization for CIDStore
export CIDSTORE_AUTO_OPTIMIZE=1
echo "✓ Auto-optimization enabled (CIDSTORE_AUTO_OPTIMIZE=1)"

echo ""
echo "Python 3.13 optimizations are now configured."
echo ""

if [ "$supportsGIL" = "True" ]; then
    echo "To make this permanent, add these lines to your ~/.bashrc or ~/.zshrc:"
    echo '  export PYTHON_GIL=0'
    echo '  export PYTHON_JIT=1'
    echo '  export CIDSTORE_AUTO_OPTIMIZE=1'
else
    echo "To make JIT permanent, add these lines to your ~/.bashrc or ~/.zshrc:"
    echo '  export PYTHON_JIT=1'
    echo '  export CIDSTORE_AUTO_OPTIMIZE=1'
fi

echo ""
echo "To verify configuration, run: python pyproject_config.py"
