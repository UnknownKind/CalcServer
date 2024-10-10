cmake_minimum_required(VERSION 3.14)

project(CalcServer LANGUAGES CXX)

set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_SHARED_LIBRARY_PREFIX "")

set(SOURCE_LIB  
    CalcServer.cpp
    ${CMAKE_SOURCE_DIR}/src/Loaders/LoadData.cpp
    ${CMAKE_SOURCE_DIR}/src/Logger/Logger.cpp
    ${CMAKE_SOURCE_DIR}/src/DatabaseManagements/basic_structures/basic_structures.cpp
)

add_library(${PROJECT_NAME} SHARED ${SOURCE_LIB})

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_definitions(${PROJECT_NAME} PRIVATE DEBUG)
    message(STATUS "Adding DEBUG definition")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_definitions(${PROJECT_NAME} PRIVATE RELEASE)
    message(STATUS "Adding RELEASE definition")
endif()

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}/src/DatabaseManagements
    ${CMAKE_SOURCE_DIR}/src/Logger
    ${CMAKE_SOURCE_DIR}/src/Loaders
)

target_link_directories(${PROJECT_NAME} PUBLIC 
    ${CMAKE_SOURCE_DIR}/lib
    ${CMAKE_SOURCE_DIR}/src/DatabaseManagements
)

target_link_libraries(${PROJECT_NAME}
    DatabaseManagements
)

add_custom_command(
    TARGET ${PROJECT_NAME} POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/ConfigDB.json ${CMAKE_BINARY_DIR}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ConfigDB.json
)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.dll DESTINATION ${CMAKE_BINARY_DIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}.dll DESTINATION ${CMAKE_SOURCE_DIR}/lib)
