hadoop
======


Exercise 1 source code: WordCount1

Exercise 2 source code: WordCount2

Exercise 3 source code: WordCount3

Notice1:The code for Exercise 3 can only sort all the 7-character words by their frequency, but cant pick top 100s for now. You may use command bin/hadoop fs -cat /output/part-r-00000 | head n100 to get the corrrect result. 
I am still working on the code. 

Notice2: If you want to compile the code WordCount3.java. Please use the command"javac -cp hadoop-1.0.3/hadoop-core-1.0.3.jar:hadoop-1.0.3/lib/commons-cli-1.2.jar WordCount3.java"
