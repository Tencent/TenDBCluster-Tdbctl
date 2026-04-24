#!/bin/bash
# Download and extract ncurses source code into extra/ncurses-<version>/
# Usage: bash extra/download_ncurses.sh [-v <version>]
# Example:
#   bash extra/download_ncurses.sh              # default: 5.7
#   bash extra/download_ncurses.sh -v 6.4       # download ncurses 6.4
#   bash extra/download_ncurses.sh --version 5.9

set -e

# ---------- defaults ----------
VERSION="5.7"

# ---------- parse arguments ----------
usage() {
    echo "Usage: $0 [-v|--version <version>]"
    echo ""
    echo "Options:"
    echo "  -v, --version <version>   ncurses version to download (default: ${VERSION})"
    echo "  -h, --help                show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                  # download ncurses 5.7"
    echo "  $0 -v 6.4           # download ncurses 6.4"
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# ---------- paths ----------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TARGET_DIR="${SCRIPT_DIR}/ncurses-${VERSION}"
TARBALL="${SCRIPT_DIR}/ncurses-${VERSION}.tar.gz"

# ---------- check if already exists ----------
if [ -d "${TARGET_DIR}" ] && [ -f "${TARGET_DIR}/configure" ]; then
    echo "ncurses-${VERSION} source already exists at ${TARGET_DIR}"
    exit 0
fi

echo "==> Downloading ncurses ${VERSION} ..."

# ---------- download function ----------
download_file() {
    local url="$1"
    local dest="$2"
    if command -v wget &> /dev/null; then
        wget -O "${dest}" "${url}" && return 0
    elif command -v curl &> /dev/null; then
        curl -L -o "${dest}" "${url}" && return 0
    fi
    return 1
}

# ---------- try multiple sources ----------
DOWNLOADED=0

# Source 1: GNU FTP (official)
GNU_URL="https://ftp.gnu.org/pub/gnu/ncurses/ncurses-${VERSION}.tar.gz"
echo "    Trying GNU FTP: ${GNU_URL}"
if download_file "${GNU_URL}" "${TARBALL}" 2>/dev/null; then
    if [ -f "${TARBALL}" ] && [ -s "${TARBALL}" ]; then
        DOWNLOADED=1
    fi
fi

# Source 2: GitHub mirror
if [ "${DOWNLOADED}" -eq 0 ]; then
    GITHUB_URL="https://github.com/mirror/ncurses/archive/refs/tags/v${VERSION}.tar.gz"
    echo "    Trying GitHub mirror: ${GITHUB_URL}"
    if download_file "${GITHUB_URL}" "${TARBALL}" 2>/dev/null; then
        if [ -f "${TARBALL}" ] && [ -s "${TARBALL}" ]; then
            DOWNLOADED=1
        fi
    fi
fi

# Source 3: invisible-island (ncurses upstream)
if [ "${DOWNLOADED}" -eq 0 ]; then
    INVISIBLE_URL="https://invisible-mirror.net/archives/ncurses/ncurses-${VERSION}.tar.gz"
    echo "    Trying invisible-mirror: ${INVISIBLE_URL}"
    if download_file "${INVISIBLE_URL}" "${TARBALL}" 2>/dev/null; then
        if [ -f "${TARBALL}" ] && [ -s "${TARBALL}" ]; then
            DOWNLOADED=1
        fi
    fi
fi

if [ "${DOWNLOADED}" -eq 0 ]; then
    echo "ERROR: Failed to download ncurses ${VERSION} from all sources."
    echo "Please manually download ncurses-${VERSION}.tar.gz and place it at:"
    echo "  ${TARBALL}"
    echo "Then re-run this script."
    rm -f "${TARBALL}"
    exit 1
fi

# ---------- extract ----------
echo "==> Extracting ncurses ${VERSION} ..."
cd "${SCRIPT_DIR}"
tar xfz "ncurses-${VERSION}.tar.gz"

# Handle different directory names from different archive sources:
#   GNU FTP:    ncurses-5.7/
#   GitHub tag: ncurses-5.7/ or ncurses-v5.7/
for candidate in "ncurses-v${VERSION}" "ncurses-${VERSION}-${VERSION}"; do
    if [ -d "${candidate}" ] && [ ! -d "ncurses-${VERSION}" ]; then
        mv "${candidate}" "ncurses-${VERSION}"
    fi
done

# ---------- verify ----------
if [ ! -f "${TARGET_DIR}/configure" ]; then
    echo "ERROR: Extraction succeeded but configure script not found."
    echo "Please check the contents of ${TARGET_DIR}/"
    rm -f "${TARBALL}"
    exit 1
fi

# ---------- cleanup ----------
rm -f "${TARBALL}"

echo "==> ncurses ${VERSION} source code is ready at ${TARGET_DIR}"
