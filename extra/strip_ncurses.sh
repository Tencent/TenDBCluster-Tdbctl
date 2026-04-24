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
#   - doc/          : documentation
#   - progs/        : utility programs (tic, toe, infocmp, etc.)
#
# What we KEEP (for configure script compatibility):
#   - man/          : man pages (required by configure script)
#   - test/         : test programs (required by configure script)
#   - panel/        : panel library (required by configure script)
#   - form/         : form library (required by configure script)
#   - menu/         : menu library (required by configure script)
#
# What we REMOVE (not needed, can cause build issues):
#   - c++/          : C++ bindings (not needed, may conflict with system ncurses++)
#   - Ada95/        : Ada bindings (not needed)
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
#   - doc/      : documentation
#   - progs/    : utility programs (tic, toe, infocmp, etc.)

# KEEP these directories for configure script compatibility:
#   - man/      : man pages (required by configure)
#   - test/     : test programs (required by configure)  
#   - panel/    : panel library (required by configure)
#   - form/     : form library (required by configure)
#   - menu/     : menu library (required by configure)

echo "    Removing unnecessary directories..."
rm -rf doc/
rm -rf progs/
rm -rf c++/
rm -rf Ada95/

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
echo "  misc/             - build configuration files (includes terminfo.src)"
echo "  man/              - man pages (required for configure)"
echo "  test/             - test programs (required for configure)"
echo "  panel/            - panel library (required for configure)"
echo "  form/             - form library (required for configure)"
echo "  menu/             - menu library (required for configure)"
echo "  configure         - build script"
echo "  Makefile*         - build rules"
echo "  *.m4              - autoconf macros"
