#!/usr/bin/env bash

set -ex

mvn dependency:resolve -Dclassifier=javadoc
mvn dependency:sources
mvn eclipse:eclipse
