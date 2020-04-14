# CS-455-HW3
**Title**: Analyzing Air Quality Data Collected across the United States using MapReduce

**Author**: Joshua Burris (all code was written from scratch by me)

**Assignment Page**: [Local PDF](CS455-Spring20-HW3-PC.pdf)

## File Descriptions

   **books/**: A folder for the bookes I used when running the word count example.
   **build/**: These are the output files that are generated when you build the project (see Building section).
   **clientConf/**: The configuration files for the shared HDFS cluster.
   **hadoopConf/**: The configuration files for my local HDFS cluster.
   **Q1-out/ ... Q6-out/**: The output files generated by my program, answering the questions in the assignment.
   **src/main/java/cs455/hadoop/**: The source for my java files.
   **build.gradle**: The file used to build the .class and .jar files needed to run my program.
   **Makefile**: A great way to execute my program without typing a lot.
   **report.pdf**: My report on the questions the assignment wanted answered.

## Building

<code>**CS-455-HW3$** gradle build</code>

## Executing

My Makefile is built as a nice shortcut for executing, although it also contains script to delete the hdfs output file and local output file before the script runs into an error for a file already existing. It also automatically copies the output file generated to our local disk.

### Q1:

<code>**CS-455-HW3$** make Q1</code>

OR

${HADOOP_HOME}/bin/hadoop jar ./build/libs/CS-455-HW3.jar cs455.hadoop.q.Q1 /data/gases/ /data/meteorological/ /home/Q1-out/

### Q2:

<code>**CS-455-HW3$** make Q2</code>

OR

${HADOOP_HOME}/bin/hadoop jar ./build/libs/CS-455-HW3.jar cs455.hadoop.q.Q2 /data/gases/ /home/Q2-out/

### Q3:

<code>**CS-455-HW3$** make Q3</code>

OR

${HADOOP_HOME}/bin/hadoop jar ./build/libs/CS-455-HW3.jar cs455.hadoop.q.Q3 /data/gases/ /home/Q3-out/

### Q4:

<code>**CS-455-HW3$** make Q4</code>

OR

${HADOOP_HOME}/bin/hadoop jar ./build/libs/CS-455-HW3.jar cs455.hadoop.q.Q4 /data/gases/ /home/Q4-out/

### Q5:

<code>**CS-455-HW3$** make Q5</code>

OR

${HADOOP_HOME}/bin/hadoop jar ./build/libs/CS-455-HW3.jar cs455.hadoop.q.Q5 /data/meteorological/ /home/Q5-out/

### Q6:

<code>**CS-455-HW3$** make Q6</code>

OR

${HADOOP_HOME}/bin/hadoop jar ./build/libs/CS-455-HW3.jar cs455.hadoop.q.Q6 /data/gases/ /home/Q6-out/
