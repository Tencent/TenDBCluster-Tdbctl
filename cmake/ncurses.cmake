# Copyright (c) 2020, Tencent and/or its affiliates. All rights reserved.

# Build bundled ncurses 5.7 from source (located in extra/ncurses-5.7)
# to remove dependency on system ncurses.
#
# Uses ExternalProject_Add to build ncurses during the build phase
# (not configure phase), which is the standard CMake approach.

INCLUDE(ExternalProject)

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

  # Detect number of CPUs for parallel build
  INCLUDE(ProcessorCount)
  ProcessorCount(NCPU)
  IF(NOT NCPU OR NCPU EQUAL 0)
    SET(NCPU 4)
  ENDIF()

  # Build ncurses using ExternalProject_Add (builds during make, not cmake)
  ExternalProject_Add(ncurses_external
    SOURCE_DIR "${BUNDLED_NCURSES_SRC}"
    BINARY_DIR "${BUNDLED_NCURSES_BUILD}"
    INSTALL_DIR "${BUNDLED_NCURSES_INSTALL}"
    
    # Configure step - build a minimal ncurses with only the features we need
    CONFIGURE_COMMAND "${BUNDLED_NCURSES_SRC}/configure"
      "--prefix=<INSTALL_DIR>"
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
    
    # Build step
    BUILD_COMMAND make -j${NCPU}
    
    # Install step
    INSTALL_COMMAND make install
    
    # Avoid repeated configure when stamp files exist
    BUILD_BYPRODUCTS
      "${BUNDLED_NCURSES_INSTALL}/lib/libncurses.a"
      "${BUNDLED_NCURSES_INSTALL}/lib/libtinfo.a"
  )

  # Set variables for the rest of the build system
  # These point to where the libraries will be after ncurses_external is built
  SET(CURSES_FOUND TRUE)

  # ncurses 5.7 installs headers to include/ncurses/ directory
  # The bundled curses.h uses #include <ncurses/unctrl.h> style includes,
  # so we need to set include path to the parent directory (include/)
  # rather than include/ncurses/ for the includes to work correctly.
  SET(CURSES_INCLUDE_PATH "${BUNDLED_NCURSES_INSTALL}/include"
    CACHE PATH "Path to bundled ncurses include directory" FORCE)

  SET(CURSES_CURSES_H_PATH "${CURSES_INCLUDE_PATH}"
    CACHE PATH "" FORCE)

  # Set library paths (will exist after ncurses_external builds)
  SET(CURSES_LIBRARY "${BUNDLED_NCURSES_INSTALL}/lib/libncurses.a"
    CACHE FILEPATH "Path to bundled ncurses library" FORCE)
  SET(CURSES_CURSES_LIBRARY "${BUNDLED_NCURSES_INSTALL}/lib/libncurses.a"
    CACHE FILEPATH "" FORCE)
  SET(CURSES_TINFO_LIBRARY "${BUNDLED_NCURSES_INSTALL}/lib/libtinfo.a"
    CACHE FILEPATH "Path to bundled tinfo library" FORCE)

  SET(CURSES_HAVE_CURSES_H FALSE)
  SET(CURSES_HAVE_NCURSES_H TRUE)
  SET(HAVE_NCURSES_H 1 CACHE INTERNAL "")

  SET(USING_BUNDLED_NCURSES TRUE CACHE INTERNAL "Using bundled ncurses" FORCE)

  # Export the target name so libedit can add dependency
  SET(NCURSES_EXTERNAL_TARGET ncurses_external PARENT_SCOPE)

  MESSAGE(STATUS "Using bundled ncurses (will build during make)")
  MESSAGE(STATUS "  CURSES_INCLUDE_PATH=${CURSES_INCLUDE_PATH}")
  MESSAGE(STATUS "  CURSES_LIBRARY=${CURSES_LIBRARY}")
  MESSAGE(STATUS "  CURSES_TINFO_LIBRARY=${CURSES_TINFO_LIBRARY}")
ENDMACRO()
