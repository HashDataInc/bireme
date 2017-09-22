set -xeu

examine_result() {
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from numericsource order by id;" >> source.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from charsource order by id;" >> source.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from timesource order by id;" >> source.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from binarysource order by id;" >> source.txt

	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from numerictarget order by id;" >> target.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from chartarget order by id;" >> target.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from timetarget order by id;" >> target.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from binarytarget order by id;" >> target.txt

	if [[ -z $(diff source.txt target.txt) ]]; then
		echo "Data are identical!"
		true
	else
		echo "Data are different!"
		false
	fi
}

examine_result