set -xeu

export CI_DIR=${PWD}/integration_test
export SOURCE_DIR=${CI_DIR}/${SOURCE}

cp -f ${SOURCE_DIR}/pom.xml ./pom.xml

mvn docker:start
mvn clean package -Dmaven.test.skip=true

tar -xf $(ls target/*.tar.gz) -C target
export BIREME=$(ls target/*.tar.gz | awk '{print i$0}' i=$(pwd)'/' | sed -e "s/.tar.gz$//")

rm -rf ${BIREME}/etc
cp -rf ${SOURCE_DIR}/etc ${BIREME}/etc
