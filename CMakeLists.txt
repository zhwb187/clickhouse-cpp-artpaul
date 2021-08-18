CMAKE_MINIMUM_REQUIRED(VERSION 3.0.2)

INCLUDE (cmake/cpp11.cmake)
INCLUDE (cmake/subdirs.cmake)

OPTION(BUILD_BENCHMARK "Build benchmark" OFF)
OPTION(BUILD_TESTS "Build tests" OFF)

PROJECT (CLICKHOUSE-CLIENT)

    USE_CXX11()

	IF ("${CMAKE_BUILD_TYPE}" STREQUAL "")
		set(CMAKE_BUILD_TYPE "Release")
	ENDIF()

    IF (UNIX)
        IF ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
            SET (GO_FLAG "-O2")
        ELSE ()
            SET (GO_FLAG "-g")
        ENDIF ()
        IF (APPLE)
            SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GO_FLAG} -Wall -Wextra -Werror")
        ELSE ()
            SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GO_FLAG} -pthread -Wall -Wextra -Werror")
        ENDIF ()
        SET (CMAKE_EXE_LINKER_FLAGS, "${CMAKE_EXE_LINKER_FLAGS} -lpthread")
    ENDIF ()

    INCLUDE_DIRECTORIES(.)
    INCLUDE_DIRECTORIES(contrib)
    INCLUDE_DIRECTORIES(std)

    SUBDIRS (
        clickhouse
        contrib/absl
        contrib/cityhash
        contrib/lz4
    )

    IF (BUILD_BENCHMARK)
        SUBDIRS(bench)
    ENDIF (BUILD_BENCHMARK)

    IF (BUILD_TESTS)
        SUBDIRS(
            contrib/gtest
            tests/simple
            ut
        )
	ENDIF (BUILD_TESTS)
