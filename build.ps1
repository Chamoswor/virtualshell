param (
    [string]$buildType = "both"  # Default to both
)

switch ($buildType) {
    "--sdist" {
        $BUILD_SDIST = $true
        $BUILD_WHEEL = $false
    }
    "--wheel" {
        $BUILD_SDIST = $false
        $BUILD_WHEEL = $true
    }
    "both" {
        $BUILD_SDIST = $true
        $BUILD_WHEEL = $true
    }
    default {
        Write-Host "Usage: $0 [--sdist|--wheel]"
        exit 1
    }
}

python -m venv .venv_build
.venv_build\Scripts\activate

if ($BUILD_WHEEL) {
    python -m pip install --upgrade pip setuptools wheel cibuildwheel

    # Hard reset av potensielle globale innstillinger som forstyrrer
    Remove-Item Env:CIBW_ARCHS -ErrorAction SilentlyContinue
    Remove-Item Env:CIBW_ARCHS_MACOS -ErrorAction SilentlyContinue
    Remove-Item Env:CIBW_ARCHS_LINUX -ErrorAction SilentlyContinue
    Remove-Item Env:CIBW_ARCHS_WINDOWS -ErrorAction SilentlyContinue

    # Windows trenger AMD64/ARM64/x86 – sett eksplisitt AMD64
    $env:CMAKE_GENERATOR="Visual Studio 17 2022"
    $env:CMAKE_GENERATOR_PLATFORM="x64"
    $env:PYBIND11_FINDPYTHON="ON"
    $env:CIBW_ARCHS="AMD64"
    $env:CIBW_BUILD="cp38-win_amd64 cp39-win_amd64 cp310-win_amd64 cp311-win_amd64 cp312-win_amd64 cp313-win_amd64"
    $env:CIBW_SKIP="*-win32"
    $env:CIBW_ENVIRONMENT_WINDOWS='CMAKE_GENERATOR="Visual Studio 17 2022" CMAKE_GENERATOR_PLATFORM=x64 PYBIND11_FINDPYTHON=ON'
    $env:CIBW_TEST_COMMAND = "python -c `"import virtualshell; print('ok')`""

    # Du kan droppe --platform på Windows; autodeteksjon funker fint.
    python -m cibuildwheel --output-dir dist/ .
}


if ($BUILD_SDIST) {
    mkdir temp

    Copy-Item README.md, LICENSE, demo.py, pyproject.toml, CMakeLists.txt -Destination temp/
    Copy-Item -Recurse src/ -Destination temp/src/
    Copy-Item -Recurse cpp/ -Destination temp/cpp/

    Set-Location temp/
    python -m pip install --upgrade build
    python -m build --sdist --outdir ../dist/ .
    Set-Location ..

    Remove-Item -Recurse -Force temp/
}