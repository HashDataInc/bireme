set -xeu

export SOURCE_DIR=${PWD}/integration_test/${SOURCE}
export DOCKER_EXEC='docker exec -it'

${SOURCE_DIR}/prepare.sh

mvn clean package -Dmaven.test.skip=true

tar -xf $(ls target/*.tar.gz) -C target
BIREME=$(ls target/*.tar.gz | awk '{print i$0}' i=$(pwd)'/' | sed -e "s/.tar.gz$//")

rm -rf ${BIREME}/etc
cp -rf ${SOURCE_DIR}/etc ${BIREME}/etc


${BIREME}/bin/bireme start
sleep 20
${BIREME}/bin/bireme stop

rm -f source.txt target.txt
touch source.txt target.txt

${SOURCE_DIR}/examine.sh
