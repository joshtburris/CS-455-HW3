HJAVAC=${HADOOP_HOME}/bin/hadoop com.sun.tools.javac
HGET=-${HADOOP_HOME}/bin/hadoop fs -get
HJAR=${HADOOP_HOME}/bin/hadoop jar
HREMOVE=-${HADOOP_HOME}/bin/hadoop fs -rm -R

test:
	echo $@

build:
	gradle build

wordcount:
	-rm -r $@-out
	${HREMOVE} /home/$@-out/
	${HJAR} ./build/libs/CS-455-HW3.jar cs455.hadoop.wordcount.WordCountJob /home/books/ /home/$@-out/
	${HGET} /home/$@-out/ ./$@-out

Q1:
	-rm -r $@-out
	${HREMOVE} /home/$@-out/
	${HJAR} ./build/libs/CS-455-HW3.jar cs455.hadoop.q.$@ /data/gases/ /data/meteorological/ /home/$@-out/
	${HGET} /home/$@-out/ ./$@-out

Q2:
	-rm -r $@-out
	${HREMOVE} /home/$@-out/
	${HJAR} ./build/libs/CS-455-HW3.jar cs455.hadoop.q.$@ /data/gases/ /home/$@-out/
	${HGET} /home/$@-out/ ./$@-out

Q3:
	-rm -r $@-out
	${HREMOVE} /home/$@-out/
	${HJAR} ./build/libs/CS-455-HW3.jar cs455.hadoop.q.$@ /data/gases/ /home/$@-out/
	${HGET} /home/$@-out/ ./$@-out

Q4:
	-rm -r $@-out
	${HREMOVE} /home/$@-out/
	${HJAR} ./build/libs/CS-455-HW3.jar cs455.hadoop.q.$@ /data// /home/$@-out/
	${HGET} /home/$@-out/ ./$@-out

Q5:
	-rm -r $@-out
	${HREMOVE} /home/$@-out/
	${HJAR} ./build/libs/CS-455-HW3.jar cs455.hadoop.q.$@ /data// /home/$@-out/
	${HGET} /home/$@-out/ ./$@-out

Q6:
	-rm -r $@-out
	${HREMOVE} /home/$@-out/
	${HJAR} ./build/libs/CS-455-HW3.jar cs455.hadoop.q.$@ /data// /home/$@-out/
	${HGET} /home/$@-out/ ./$@-out
