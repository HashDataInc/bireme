set -xeu

CI_DIR=${PWD}/integration_test
SOURCE_DIR=${CI_DIR}/${SOURCE}

cp -f ${SOURCE_DIR}/etc/pom.xml ./pom.xml

mvn docker:start
mvn clean package

tar -xf $(ls target/*.tar.gz) -C target
BIREME=$(ls target/*.tar.gz | awk '{print i$0}' i=$(pwd)'/' | sed -e "s/.tar.gz$//")

rm -rf ${BIREME}/etc
cp -rf ${ETC_DIR}/etc ${BIREME}/etc
