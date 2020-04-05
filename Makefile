CUR_DIR=~/IdeaProjects/CS-455-HW3
HJAVAC=${HADOOP_HOME}/bin/hadoop com.sun.tools.javac
HGET=-${HADOOP_HOME}/bin/hadoop fs -get
HJAR=${HADOOP_HOME}/bin/hadoop jar
HREMOVE=-${HADOOP_HOME}/bin/hadoop fs -rm -R

wordcount:
	#build gradle
	-rm -r wordcount-out
	${HREMOVE} /home/wordcount-out/
	${HJAR} ${CUR_DIR}/wordcount/build/libs/cs455-wordcount-sp19.jar cs455.hadoop.wordcount.WordCountJob /home/books/ /home/wordcount-out/
	${HGET} /home/wordcount-out/ ${CUR_DIR}/wordcount-out

Q1:
	${HREMOVE} /home/Q1-out
	
	${HJAVAC}.Main ${CUR_DIR}/Q1/Q1.java
	jar cf ${CUR_DIR}/Q1/Q1.jar ${CUR_DIR}/Q1/Q1 ${CUR_DIR}/Q1/*.class
	${HJAR} ${CUR_DIR}/Q1/Q1.jar ${CUR_DIR}/Q1/Q1 /data/gases /home/Q1-out
	
	${HGET} /home/Q1-out ${CUR_DIR}/Q1-out


