echo "Let's go"

rm input/*
wait

python generatorData.py
wait

mv input_file input/

wait

echo "Removing old input and output"

hdfs dfs -rm -r output
wait

hdfs dfs -rm -r input
wait

echo "Loading data"
hdfs dfs -put input input
wait

echo "Run Java program"

yarn jar /root/.m2/repository/bdtc/lab1/1.0-SNAPSHOT/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output
wait

echo "Still running"

hadoop fs -text output/part-r-00000 > results.txt
wait

echo "Done"

cat results.txt
wait
