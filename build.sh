#!/bin/bash

# Check args, only --sdist, or only --wheel, or both (default)
if [ "$1" = "--sdist" ]; then
    BUILD_SDIST=true
    BUILD_WHEEL=false
elif [ "$1" = "--wheel" ]; then
    BUILD_SDIST=false
    BUILD_WHEEL=true
elif [ -n "$1" ]; then
    echo "Usage: $0 [--sdist|--wheel]"
    exit 1
else
    BUILD_SDIST=true
    BUILD_WHEEL=true
fi

python3 -m venv .venv_build
. .venv_build/bin/activate

if [ "$BUILD_WHEEL" = true ]; then
    python -m pip install --upgrade pip setuptools wheel cibuildwheel

    CIBW_BUILD="cp38-* cp39-* cp310-* cp311-* cp312-* cp313-*" \
    CIBW_SKIP="*-musllinux_* *_i686" \
    CIBW_TEST_COMMAND="python -c \"import virtualshell; print('ok')\"" \
    python -m cibuildwheel --platform linux --output-dir dist/ .
fi


if [ "$BUILD_SDIST" = true ]; then
    mkdir -p temp/

    cp README.md LICENSE demo.py pyproject.toml CMakeLists.txt temp/
    cp -r src/ temp/
    cp -r cpp/ temp/

    cd temp/
    python -m pip install --upgrade build
    python -m build --sdist --outdir ../dist/ .
    cd ..
    rm -rf temp/
fi