CUR_DIR=~/IdeaProjects/CS-455-HW3
HJAVAC=${HADOOP_HOME}/bin/hadoop com.sun.tools.javac
HGET=-${HADOOP_HOME}/bin/hadoop fs -get
HJAR=${HADOOP_HOME}/bin/hadoop jar
HREMOVE=-${HADOOP_HOME}/bin/hadoop fs -rm -R

all:
	-rm -R ${CUR_DIR}/Q1-out
	${HREMOVE} /home/Q1-out
	
	${HJAVAC}.Main Q1.java
	jar cf Q1.jar Q1 *.class
	${HJAR} Q1.jar Q1 /data/ /home/Q1-out
	
	${HGET} /home/Q1-out ${CUR_DIR}/Q1-out
