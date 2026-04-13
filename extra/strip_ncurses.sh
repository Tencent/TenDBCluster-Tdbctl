#!/bin/bash
# Strip unnecessary files from ncurses source tree to reduce repository size.
# We keep all files required to build libncurses and libtinfo static libraries.
#
# What we KEEP:
#   - ncurses/      : core library source including:
#       - base/     : ncurses core functions
#       - tinfo/    : terminfo/termcap library (libtinfo)
#       - tty/      : terminal I/O
#       - trace/    : debugging/trace support
#       - widechar/ : wide character support
#   - include/      : header files
#   - misc/         : build configuration files
#   - configure     : build script
#   - Makefile*     : build rules
#   - *.m4          : autoconf macros
#
# What we REMOVE (not needed for libncurses/libtinfo):
#   - Ada95/        : Ada bindings
#   - c++/          : C++ bindings
#   - doc/          : documentation
#   - man/          : man pages
#   - test/         : test programs
#   - panel/        : panel library
#   - form/         : form library
#   - menu/         : menu library
#   - progs/        : utility programs (tic, toe, infocmp, etc.)
#
# Usage: bash extra/strip_ncurses.sh [-v <version>]
# Example:
#   bash extra/strip_ncurses.sh              # default: 5.7
#   bash extra/strip_ncurses.sh -v 6.4       # strip ncurses 6.4

set -e

# ---------- defaults ----------
VERSION="5.7"

# ---------- parse arguments ----------
usage() {
    echo "Usage: $0 [-v|--version <version>]"
    echo ""
    echo "Options:"
    echo "  -v, --version <version>   ncurses version to strip (default: ${VERSION})"
    echo "  -h, --help                show this help message"
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

# ---------- check if exists ----------
if [ ! -d "${TARGET_DIR}" ]; then
    echo "ERROR: ncurses-${VERSION} source not found at ${TARGET_DIR}"
    echo "Please run download_ncurses.sh first."
    exit 1
fi

echo "==> Stripping unnecessary files from ncurses ${VERSION} ..."
echo "    Keeping only files required to build libncurses and libtinfo"

cd "${TARGET_DIR}"

# ---------- remove unnecessary directories ----------
# These are not needed for building libncurses/libtinfo:
#   - Ada95/    : Ada bindings
#   - c++/      : C++ bindings
#   - doc/      : documentation
#   - man/      : man pages
#   - test/     : test programs
#   - panel/    : panel library
#   - form/     : form library
#   - menu/     : menu library
#   - progs/    : utility programs (tic, toe, infocmp, etc.)

echo "    Removing unnecessary directories..."
rm -rf Ada95/
rm -rf c++/
rm -rf doc/
rm -rf man/
rm -rf test/
rm -rf panel/
rm -rf form/
rm -rf menu/
rm -rf progs/

# ---------- remove unnecessary top-level files ----------
echo "    Removing documentation files..."
rm -f ANNOUNCE
rm -f announce.html.in
rm -f NEWS
rm -f README
rm -f README.emx
rm -f TO-DO
rm -f INSTALL
rm -f MANIFEST

# Remove OS/2 specific files
rm -f Makefile.os2

# ---------- remove ncurses internal llib files ----------
# These are lint library stubs, not needed for build
echo "    Removing lint library files..."
rm -f ncurses/llib-lncurses
rm -f ncurses/llib-lncursest
rm -f ncurses/llib-lncursesw

# ---------- remove large terminfo source ----------
# We use --with-fallbacks during configure instead of full terminfo database
echo "    Removing terminfo source (using fallbacks instead)..."
rm -f misc/terminfo.src

echo ""
echo "==> Done! Stripped ncurses ${VERSION} at ${TARGET_DIR}"
echo ""
echo "Remaining structure (sufficient for building libncurses and libtinfo):"
echo "  ncurses/"
echo "    ├── base/       - ncurses core functions"
echo "    ├── tinfo/      - terminfo/termcap library (libtinfo)"
echo "    ├── tty/        - terminal I/O"
echo "    ├── trace/      - debugging/trace support"
echo "    └── widechar/   - wide character support"
echo "  include/          - header files"
echo "  misc/             - build configuration files"
echo "  configure         - build script"
echo "  Makefile*         - build rules"
echo "  *.m4              - autoconf macros"
