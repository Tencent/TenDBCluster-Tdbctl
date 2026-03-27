# Copyright (c) 2024, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# Build bundled ncurses 5.7 from source (located in extra/ncurses-5.7)
# to remove dependency on system ncurses.

SET(BUNDLED_NCURSES_SRC "${CMAKE_SOURCE_DIR}/extra/ncurses-5.7")
SET(BUNDLED_NCURSES_BUILD "${CMAKE_BINARY_DIR}/bundled_ncurses/build")
SET(BUNDLED_NCURSES_INSTALL "${CMAKE_BINARY_DIR}/bundled_ncurses/install")

MACRO(MYSQL_BUILD_BUNDLED_NCURSES)
  # Verify source code exists
  IF(NOT EXISTS "${BUNDLED_NCURSES_SRC}/configure")
    MESSAGE(FATAL_ERROR
      "Bundled ncurses source not found at ${BUNDLED_NCURSES_SRC}\n"
      "Please run: bash extra/download_ncurses.sh [-v <version>]\n"
      "  e.g.: bash extra/download_ncurses.sh          (default: 5.7)\n"
      "        bash extra/download_ncurses.sh -v 6.4\n"
      "Or manually download ncurses source and extract to extra/ncurses-<version>/")
  ENDIF()

  IF(NOT EXISTS "${BUNDLED_NCURSES_INSTALL}/lib/libncurses.a")
    MESSAGE(STATUS "Building bundled ncurses from ${BUNDLED_NCURSES_SRC} ...")

    # Create build and install directories
    FILE(MAKE_DIRECTORY "${BUNDLED_NCURSES_BUILD}")
    FILE(MAKE_DIRECTORY "${BUNDLED_NCURSES_INSTALL}")

    # Configure ncurses
    # Build a minimal ncurses with only the features we need:
    #   - static library only (no shared)
    #   - no programs (tic, toe, etc.)
    #   - no manpages
    #   - with terminfo fallback (so it works without terminfo database)
    #   - position independent code for linking into shared libraries
    MESSAGE(STATUS "Configuring ncurses...")
    EXECUTE_PROCESS(
      COMMAND "${BUNDLED_NCURSES_SRC}/configure"
        "--prefix=${BUNDLED_NCURSES_INSTALL}"
        "--without-shared"
        "--with-normal"
        "--without-debug"
        "--without-ada"
        "--without-manpages"
        "--without-progs"
        "--without-tests"
        "--without-cxx"
        "--without-cxx-binding"
        "--with-termlib"
        "--with-fallbacks=ansi,cons25,dumb,linux,rxvt,screen,sun,vt100,vt102,vt220,xterm,xterm-256color,xterm-color"
        "--enable-termcap"
        "CFLAGS=-fPIC"
        "CPPFLAGS=-fPIC"
      WORKING_DIRECTORY "${BUNDLED_NCURSES_BUILD}"
      RESULT_VARIABLE NCURSES_CONFIGURE_RESULT
    )
    IF(NOT NCURSES_CONFIGURE_RESULT EQUAL 0)
      MESSAGE(FATAL_ERROR "Failed to configure ncurses")
    ENDIF()

    # Build ncurses
    # Detect number of CPUs for parallel build
    INCLUDE(ProcessorCount)
    ProcessorCount(NCPU)
    IF(NOT NCPU OR NCPU EQUAL 0)
      SET(NCPU 4)
    ENDIF()

    MESSAGE(STATUS "Building ncurses with ${NCPU} parallel jobs...")
    EXECUTE_PROCESS(
      COMMAND make -j${NCPU}
      WORKING_DIRECTORY "${BUNDLED_NCURSES_BUILD}"
      RESULT_VARIABLE NCURSES_BUILD_RESULT
    )
    IF(NOT NCURSES_BUILD_RESULT EQUAL 0)
      MESSAGE(FATAL_ERROR "Failed to build ncurses")
    ENDIF()

    # Install ncurses to local prefix
    MESSAGE(STATUS "Installing ncurses to ${BUNDLED_NCURSES_INSTALL}")
    EXECUTE_PROCESS(
      COMMAND make install
      WORKING_DIRECTORY "${BUNDLED_NCURSES_BUILD}"
      RESULT_VARIABLE NCURSES_INSTALL_RESULT
    )
    IF(NOT NCURSES_INSTALL_RESULT EQUAL 0)
      MESSAGE(FATAL_ERROR "Failed to install ncurses")
    ENDIF()

    MESSAGE(STATUS "Bundled ncurses built successfully")
  ELSE()
    MESSAGE(STATUS "Using previously built bundled ncurses at ${BUNDLED_NCURSES_INSTALL}")
  ENDIF()

  # Set variables for the rest of the build system
  SET(CURSES_FOUND TRUE)

  # ncurses 5.7 installs headers to include/ncurses/ directory
  # The bundled curses.h uses #include <ncurses/unctrl.h> style includes,
  # so we need to set include path to the parent directory (include/)
  # rather than include/ncurses/ for the includes to work correctly.
  SET(CURSES_INCLUDE_PATH "${BUNDLED_NCURSES_INSTALL}/include"
    CACHE PATH "Path to bundled ncurses include directory" FORCE)

  SET(CURSES_CURSES_H_PATH "${CURSES_INCLUDE_PATH}"
    CACHE PATH "" FORCE)

  # Check which libraries were built
  IF(EXISTS "${BUNDLED_NCURSES_INSTALL}/lib/libncurses.a")
    SET(CURSES_LIBRARY "${BUNDLED_NCURSES_INSTALL}/lib/libncurses.a"
      CACHE FILEPATH "Path to bundled ncurses library" FORCE)
    SET(CURSES_CURSES_LIBRARY "${BUNDLED_NCURSES_INSTALL}/lib/libncurses.a"
      CACHE FILEPATH "" FORCE)
  ENDIF()

  # Also include libtinfo if built separately
  IF(EXISTS "${BUNDLED_NCURSES_INSTALL}/lib/libtinfo.a")
    SET(CURSES_TINFO_LIBRARY "${BUNDLED_NCURSES_INSTALL}/lib/libtinfo.a"
      CACHE FILEPATH "Path to bundled tinfo library" FORCE)
  ELSE()
    SET(CURSES_TINFO_LIBRARY "" CACHE FILEPATH "" FORCE)
  ENDIF()

  SET(CURSES_HAVE_CURSES_H FALSE)
  SET(CURSES_HAVE_NCURSES_H TRUE)
  SET(HAVE_NCURSES_H 1 CACHE INTERNAL "")

  SET(USING_BUNDLED_NCURSES TRUE CACHE INTERNAL "Using bundled ncurses" FORCE)

  MESSAGE(STATUS "Bundled ncurses: CURSES_INCLUDE_PATH=${CURSES_INCLUDE_PATH}")
  MESSAGE(STATUS "Bundled ncurses: CURSES_LIBRARY=${CURSES_LIBRARY}")
  IF(CURSES_TINFO_LIBRARY)
    MESSAGE(STATUS "Bundled ncurses: CURSES_TINFO_LIBRARY=${CURSES_TINFO_LIBRARY}")
  ENDIF()
ENDMACRO()
