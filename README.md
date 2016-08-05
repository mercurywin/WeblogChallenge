>NOTE: There're two folders, one for shell only and the other is for singletion deployed with an unit test.

#sessionizeForShell
1. unzip and change the data filename to raw.txt(it seems unnecessary to unzip the file).
2. move the data file to the home directory of spark.
3. open the spark shell.
4. copy the content from sessionization.scala to spark shell.
5. execute it then you could get the answers.

#sessionize
It's an IDEA Project based on SBT and could be runned with enough libs. Moreover there is a unit test file with the project in the src/test/scala.
There're two core files, please take a look:
1. src/main/scala/Sessionalize.scala
2. src/test/scala/SessionalizeTest.scala

