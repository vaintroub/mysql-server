# Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.
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
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

IF(NOT GMOCK_FOUND)
  RETURN()
ENDIF()

IF(SSL_DEFINES)
  ADD_DEFINITIONS(${SSL_DEFINES})
ENDIF()

INCLUDE(CTest)

INCLUDE(${PROJECT_SOURCE_DIR}/rapid/plugin/x/mysqlx_protobuf.cmake)

INCLUDE(${PROJECT_SOURCE_DIR}/rapid/plugin/x/source_files.cmake)
LIST(REMOVE_ITEM xplugin_SRC "${MYSQLX_PROJECT_DIR}/src/mysql_variables.cc")

MYSQLX_PROTOBUF_GENERATE_CPP_NAMES(protobuf_SRC ${PROTOBUF_MYSQLX_FILES})

IF(MSVC)
  ADD_COMPILE_FLAGS(${protobuf_SRC} COMPILE_FLAGS "/wd4018")
ENDIF(MSVC)

# Turn off some warning flags when compiling GUnit and proto files.
MY_CHECK_CXX_COMPILER_FLAG("-Wno-unused-local-typedefs" HAVE_NO_UNUSED_TYPEDEFS)
IF(HAVE_NO_UNUSED_TYPEDEFS)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-local-typedefs")
ENDIF()
MY_CHECK_CXX_COMPILER_FLAG("-Wunused-variable" HAVE_UNUSED_VARIABLE)
IF(HAVE_UNUSED_VARIABLE)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-variable")
ENDIF()
MY_CHECK_CXX_COMPILER_FLAG("-Wmissing-field-initializers"
  HAVE_MISSING_FIELD_INITIALIZERS)
IF(HAVE_MISSING_FIELD_INITIALIZERS)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")
ENDIF()
MY_CHECK_CXX_COMPILER_FLAG("-Wno-sign-compare" HAVE_NO_SIGN_COMPARE)
IF(HAVE_NO_SIGN_COMPARE)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sign-compare")
ENDIF()
MY_CHECK_CXX_COMPILER_FLAG("-Wunused-but-set-parameter" HAVE_UNUSED_BUT_SET)
IF(HAVE_UNUSED_BUT_SET)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-but-set-parameter")
ENDIF()
MY_CHECK_CXX_COMPILER_FLAG("-Wignored-qualifiers" HAVE_NO_IGNORED_QUALIFIERS)
IF(HAVE_NO_IGNORED_QUALIFIERS)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-ignored-qualifiers")
ENDIF()


ADD_DEFINITIONS(${GMOCK_CFLAGS} -DBOOST_ALL_NO_LIB -DBOOST_NO_AUTO_PTR -DUSE_MYSQLX_FULL_PROTO -DXPLUGIN_UNIT_TESTS)

INCLUDE_DIRECTORIES(SYSTEM
    ${BOOST_INCLUDE_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
    ${SSL_INCLUDE_DIRS}
    ${LIBEVENT_INCLUDE_DIR}
    ${GMOCK_INCLUDE_DIRS}
    ${GTEST_INCLUDE_DIR}
    ${CMAKE_BINARY_DIR}/rapid/plugin/x/protobuf
)

INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/testing/include
  ${PROJECT_SOURCE_DIR}/rapid/plugin/x/ngs/include
  ${PROJECT_SOURCE_DIR}/rapid/plugin/x/ngs/include/ngs
  ${PROJECT_SOURCE_DIR}/rapid/plugin/x/mysqlxtest_src
  ${PROJECT_SOURCE_DIR}/rapid/plugin/x/src
  ${PROJECT_SOURCE_DIR}/rapid/plugin/x
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/regex
  ${PROJECT_SOURCE_DIR}/sql
  ${PROJECT_SOURCE_DIR}/include/mysql
)

FILE(
  GLOB unit_tests_SRC
  "${CMAKE_CURRENT_SOURCE_DIR}/*_t.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/stubs/log_subsystem.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/stubs/command_service.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/stubs/security_context_service.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/stubs/sql_session_service.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/stubs/misc.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/stubs/plugin.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/test_main.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/boost_code.cc"
  "${CMAKE_CURRENT_SOURCE_DIR}/mysqlx_pb_wrapper.cc"
  "${PROJECT_SOURCE_DIR}/rapid/plugin/x/src/mysql_function_names.cc"
  "${PROJECT_SOURCE_DIR}/rapid/plugin/x/mysqlxtest_src/mysqlx_row.cc"
)

ADD_EXECUTABLE(xplugin_unit_tests ${unit_tests_SRC} ${xplugin_SRC} ${protobuf_SRC})

ADD_DEPENDENCIES(xplugin_unit_tests mysqlxtest)

# New warning in Visual Studio 2008
# '%$S': virtual function overrides '%$pS', previous versions of the compiler
# did not override when parameters only differed by const/volatile qualifiers
IF(WIN32)
  SET_TARGET_PROPERTIES(xplugin_unit_tests PROPERTIES COMPILE_FLAGS "/wd4373")
ENDIF(WIN32)

TARGET_LINK_LIBRARIES(
  xplugin_unit_tests
  xplugin
  gtest
  regex
  gmock
  mysqlservices
  mysqlclient
  ${LIBEVENT_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT}
  ${PROTOBUF_LIBRARY}
  ${GCOV_LDFLAGS}
)

INCLUDE(TestGroups.txt)

