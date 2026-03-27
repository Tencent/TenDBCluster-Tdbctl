# Copyright (c) 2009, 2017, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA 

# cmake -DWITH_EDITLINE=system|bundled
# or
# cmake -DWITH_READLINE=system
# system readline is the default

MACRO (MYSQL_CHECK_MULTIBYTE)
  SET(CMAKE_EXTRA_INCLUDE_FILES wchar.h)
  CHECK_TYPE_SIZE(mbstate_t SIZEOF_MBSTATE_T)
  SET(CMAKE_EXTRA_INCLUDE_FILES)
  IF(SIZEOF_MBSTATE_T)
    SET(HAVE_MBSTATE_T 1)
  ENDIF()

  CHECK_C_SOURCE_COMPILES("
  #include <langinfo.h>
  int main(int ac, char **av)
  {
    char *cs = nl_langinfo(CODESET);
    return 0;
  }"
  HAVE_LANGINFO_CODESET)
  
  CHECK_FUNCTION_EXISTS(wcsdup HAVE_WCSDUP)

  SET(CMAKE_EXTRA_INCLUDE_FILES wchar.h)
  CHECK_TYPE_SIZE(wchar_t SIZEOF_WCHAR_T)
  IF(SIZEOF_WCHAR_T)
    SET(HAVE_WCHAR_T 1)
  ENDIF()

  SET(CMAKE_EXTRA_INCLUDE_FILES wctype.h)
  CHECK_TYPE_SIZE(wint_t SIZEOF_WINT_T)
  IF(SIZEOF_WINT_T)
    SET(HAVE_WINT_T 1)
  ENDIF()
  SET(CMAKE_EXTRA_INCLUDE_FILES)

ENDMACRO()

MACRO (FIND_CURSES)
 # Check if we should use bundled static ncurses or system dynamic ncurses
 # Default is bundled (WITH_STATIC_NCURSES=ON or undefined)
 IF(NOT DEFINED WITH_STATIC_NCURSES OR WITH_STATIC_NCURSES)
   # Use bundled ncurses (default)
   MESSAGE(STATUS "Using bundled static ncurses library")
   INCLUDE(${CMAKE_SOURCE_DIR}/cmake/ncurses.cmake)
   MYSQL_BUILD_BUNDLED_NCURSES()

   MARK_AS_ADVANCED(CURSES_CURSES_H_PATH CURSES_FORM_LIBRARY CURSES_HAVE_CURSES_H)

   IF(NOT CURSES_FOUND)
     MESSAGE(FATAL_ERROR "Failed to build bundled ncurses library.")
   ENDIF()

   # For bundled ncurses, append tinfo library if it exists
   IF(CURSES_TINFO_LIBRARY)
     SET(CURSES_LIBRARY "${CURSES_LIBRARY};${CURSES_TINFO_LIBRARY}")
   ENDIF()

   MESSAGE(STATUS "Using bundled ncurses: CURSES_LIBRARY=${CURSES_LIBRARY}")
   MESSAGE(STATUS "Using bundled ncurses: CURSES_INCLUDE_PATH=${CURSES_INCLUDE_PATH}")

   # Re-check HAVE_TERM_H with bundled ncurses include path
   # (configure.cmake may have checked this before ncurses was built)
   UNSET(HAVE_TERM_H CACHE)
   SET(SAVE_CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES})
   SET(CMAKE_REQUIRED_INCLUDES ${CURSES_INCLUDE_PATH})
   INCLUDE(CheckIncludeFiles)
   CHECK_INCLUDE_FILES(term.h HAVE_TERM_H)
   SET(CMAKE_REQUIRED_INCLUDES ${SAVE_CMAKE_REQUIRED_INCLUDES})
 ELSE()
   # Use system ncurses (dynamic linking)
   MESSAGE(STATUS "Using system dynamic ncurses library")

   # Explicitly set USING_BUNDLED_NCURSES to FALSE for system ncurses
   SET(USING_BUNDLED_NCURSES FALSE CACHE INTERNAL "Using system ncurses" FORCE)

   # Find system curses library (will find dynamic .so library)
   INCLUDE(FindCurses)

   IF(NOT CURSES_FOUND)
     MESSAGE(FATAL_ERROR "Cannot find system ncurses library. Install ncurses-devel or use --static-ncurses")
   ENDIF()

   MESSAGE(STATUS "Using system ncurses: CURSES_LIBRARY=${CURSES_LIBRARY}")
   MESSAGE(STATUS "Using system ncurses: CURSES_INCLUDE_PATH=${CURSES_INCLUDE_PATH}")

   # Check for term.h in system ncurses
   UNSET(HAVE_TERM_H CACHE)
   SET(SAVE_CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES})
   SET(CMAKE_REQUIRED_INCLUDES ${CURSES_INCLUDE_PATH})
   INCLUDE(CheckIncludeFiles)
   CHECK_INCLUDE_FILES(term.h HAVE_TERM_H)
   SET(CMAKE_REQUIRED_INCLUDES ${SAVE_CMAKE_REQUIRED_INCLUDES})
 ENDIF()
ENDMACRO()

MACRO (MYSQL_USE_BUNDLED_EDITLINE)
  SET(USE_LIBEDIT_INTERFACE 1)
  SET(HAVE_HIST_ENTRY 1)
  SET(EDITLINE_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/cmd-line-utils/libedit/editline)
  SET(EDITLINE_LIBRARY edit)
  FIND_CURSES()
  ADD_SUBDIRECTORY(${CMAKE_SOURCE_DIR}/cmd-line-utils/libedit)
ENDMACRO()

MACRO (FIND_SYSTEM_EDITLINE)
  FIND_PATH(FOUND_EDITLINE_READLINE
    NAMES editline/readline.h
  )
  IF(FOUND_EDITLINE_READLINE)
    SET(EDITLINE_INCLUDE_DIR "${FOUND_EDITLINE_READLINE}/editline")
  ELSE()
    # Different path on FreeBSD
    FIND_PATH(FOUND_EDIT_READLINE_READLINE
      NAMES edit/readline/readline.h
    )
    IF(FOUND_EDIT_READLINE_READLINE)
      SET(EDITLINE_INCLUDE_DIR "${FOUND_EDIT_READLINE_READLINE}/edit/readline")
    ENDIF()
  ENDIF()

  FIND_LIBRARY(EDITLINE_LIBRARY
    NAMES
    edit
  )
  MARK_AS_ADVANCED(EDITLINE_INCLUDE_DIR EDITLINE_LIBRARY)

  MESSAGE(STATUS "EDITLINE_INCLUDE_DIR ${EDITLINE_INCLUDE_DIR}")
  MESSAGE(STATUS "EDITLINE_LIBRARY ${EDITLINE_LIBRARY}")

  INCLUDE(CheckCXXSourceCompiles)
  IF(EDITLINE_LIBRARY AND EDITLINE_INCLUDE_DIR)
    SET(CMAKE_REQUIRED_INCLUDES ${EDITLINE_INCLUDE_DIR})
    SET(CMAKE_REQUIRED_LIBRARIES ${EDITLINE_LIBRARY})
    CHECK_CXX_SOURCE_COMPILES("
    #include <stdio.h>
    #include <readline.h>
    int main(int argc, char **argv)
    {
       HIST_ENTRY entry;
       return 0;
    }"
    EDITLINE_HAVE_HIST_ENTRY)

    CHECK_CXX_SOURCE_COMPILES("
    #include <stdio.h>
    #include <readline.h>
    int main(int argc, char **argv)
    {
      typedef int MYFunction(const char*, int);
      MYFunction* myf= rl_completion_entry_function;
      int res= (myf)(NULL, 0);
      completion_matches(0,0);
      return res;
    }"
    EDITLINE_HAVE_COMPLETION_INT)

    CHECK_CXX_SOURCE_COMPILES("
    #include <stdio.h>
    #include <readline.h>
    int main(int argc, char **argv)
    {
      typedef char* MYFunction(const char*, int);
      MYFunction* myf= rl_completion_entry_function;
      char *res= (myf)(NULL, 0);
      completion_matches(0,0);
      return res != NULL;
    }"
    EDITLINE_HAVE_COMPLETION_CHAR)

    IF(EDITLINE_HAVE_COMPLETION_INT OR EDITLINE_HAVE_COMPLETION_CHAR)
      SET(HAVE_HIST_ENTRY ${EDITLINE_HAVE_HIST_ENTRY})
      SET(USE_LIBEDIT_INTERFACE 1)
      SET(EDITLINE_FOUND 1)
      IF(EDITLINE_HAVE_COMPLETION_CHAR)
        SET(USE_NEW_XLINE_INTERFACE 1)
      ENDIF()
    ENDIF()
  ENDIF()
ENDMACRO()

MACRO (FIND_SYSTEM_READLINE)
  FIND_CURSES()
  FIND_PATH(READLINE_INCLUDE_DIR readline.h PATH_SUFFIXES readline)
  FIND_LIBRARY(READLINE_LIBRARY NAMES readline)
  MARK_AS_ADVANCED(READLINE_INCLUDE_DIR READLINE_LIBRARY)

  SET(CMAKE_REQUIRES_LIBRARIES ${${name}_LIBRARY} ${CURSES_LIBRARY})
  CHECK_INCLUDE_FILES("stdio.h;readline/readline.h;readline/history.h"
                      HAVE_READLINE_HISTORY_H)
  IF(HAVE_READLINE_HISTORY_H)
    LIST(APPEND CMAKE_REQUIRED_DEFINITIONS -DHAVE_READLINE_HISTORY_H)
  ENDIF()

  MESSAGE(STATUS "READLINE_INCLUDE_DIR ${READLINE_INCLUDE_DIR}")
  MESSAGE(STATUS "READLINE_LIBRARY ${READLINE_LIBRARY}")

  IF(READLINE_LIBRARY AND READLINE_INCLUDE_DIR)
    SET(CMAKE_REQUIRED_LIBRARIES ${READLINE_LIBRARY} ${CURSES_LIBRARY})
    SET(CMAKE_REQUIRED_INCLUDES ${READLINE_INCLUDE_DIR})
    INCLUDE(CheckCXXSourceCompiles)
    CHECK_CXX_SOURCE_COMPILES("
    #include <stdio.h>
    #include <readline.h>
    #if HAVE_READLINE_HISTORY_H
    #include <history.h>
    #endif
    int main(int argc, char **argv)
    {
       HIST_ENTRY entry;
       return 0;
    }"
    READLINE_HAVE_HIST_ENTRY)

    CHECK_CXX_SOURCE_COMPILES("
    #include <stdio.h>
    #include <readline.h>
    int main(int argc, char **argv)
    {
      CPPFunction *func1= (CPPFunction*)0;
      rl_compentry_func_t *func2= (rl_compentry_func_t*)0;
    }"
    READLINE_USE_LIBEDIT_INTERFACE)

    CHECK_CXX_SOURCE_COMPILES("
    #include <stdio.h>
    #include <readline.h>
    int main(int argc, char **argv)
    {
      rl_completion_func_t *func1= (rl_completion_func_t*)0;
      rl_compentry_func_t *func2= (rl_compentry_func_t*)0;
    }"
    READLINE_USE_NEW_READLINE_INTERFACE)

    IF(READLINE_USE_LIBEDIT_INTERFACE OR READLINE_USE_NEW_READLINE_INTERFACE)
      SET(READLINE_LIBRARY ${READLINE_LIBRARY} ${CURSES_LIBRARY})
      SET(READLINE_INCLUDE_DIR ${READLINE_INCLUDE_DIR})
      SET(HAVE_HIST_ENTRY ${READLINE_HAVE_HIST_ENTRY})
      SET(USE_LIBEDIT_INTERFACE ${READLINE_USE_LIBEDIT_INTERFACE})
      SET(USE_NEW_XLINE_INTERFACE ${READLINE_USE_NEW_READLINE_INTERFACE})
      SET(READLINE_FOUND 1)
    ENDIF()
  ENDIF()
ENDMACRO()

IF (NOT WITH_EDITLINE AND NOT WITH_READLINE AND NOT WIN32)
  SET(WITH_READLINE "system" CACHE STRING "By default use system readline")
ELSEIF (WITH_EDITLINE AND WITH_READLINE)
  MESSAGE(FATAL_ERROR "Cannot configure WITH_READLINE and WITH_EDITLINE! Use only one setting.")
ENDIF()

MACRO (MYSQL_CHECK_EDITLINE)
  IF (NOT WIN32)
    MYSQL_CHECK_MULTIBYTE()

    IF(WITH_READLINE STREQUAL "system")
      FIND_SYSTEM_READLINE()
      IF(NOT READLINE_FOUND)
        MESSAGE(FATAL_ERROR "Cannot find system readline libraries.")
      ELSE()
        SET(MY_READLINE_INCLUDE_DIR ${READLINE_INCLUDE_DIR})
        SET(MY_READLINE_LIBRARY ${READLINE_LIBRARY} ${CURSES_LIBRARY})
      ENDIF()
    ELSEIF(WITH_READLINE STREQUAL "bundled")
      MESSAGE(FATAL_ERROR "Bundled readline is not supported.")
    ELSEIF(WITH_EDITLINE STREQUAL "bundled")
      MYSQL_USE_BUNDLED_EDITLINE()
      SET(MY_READLINE_INCLUDE_DIR ${EDITLINE_INCLUDE_DIR})
      SET(MY_READLINE_LIBRARY ${EDITLINE_LIBRARY})
    ELSEIF(WITH_EDITLINE STREQUAL "system")
      FIND_SYSTEM_EDITLINE()
      IF(NOT EDITLINE_FOUND)
        MESSAGE(FATAL_ERROR "Cannot find system editline libraries.")
      ELSE()
        SET(MY_READLINE_INCLUDE_DIR ${EDITLINE_INCLUDE_DIR})
        SET(MY_READLINE_LIBRARY ${EDITLINE_LIBRARY})
      ENDIF()
    ELSE()
      MESSAGE(FATAL_ERROR "WITH_EDITLINE must be bundled or system")
    ENDIF()
  ENDIF(NOT WIN32)
ENDMACRO()

