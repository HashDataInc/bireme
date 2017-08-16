#!/usr/bin/env bash

CLANG_FORMAT=$(which clang-format 2>/dev/null || true)
CLANG_FORMAT_VERSION=$(clang-format --version 2>/dev/null || true)

if [ -z "${CLANG_FORMAT}" -o -z "${CLANG_FORMAT_VERSION}" ]; then
  echo "No valid 'clang-format' command found, skip formatting code."
else
  echo "Formatting code with 'clang-format': ${CLANG_FORMAT}"
  echo "Version: ${CLANG_FORMAT_VERSION}"

  cd ${MAVEN_PROJECTBASEDIR} && find src -name "*.java" | xargs clang-format -i -fallback-style=Google -style=file
fi

true
