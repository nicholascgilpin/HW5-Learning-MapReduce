Alexandria Stacy and Nicholas Gilpin's Mapreduce application for Tweet Analysis
Based upon the simple wordcount mapreduce application from the hadoop website. 

Step 1. Compile: hadoop com.sun.tools.javac.Main FILENAME.java
ex.  hadoop com.sun.tools.javac.Main MapreduceTime.java

Step 2. Create Jar: jar cf wc.jar FILENAME*.class
ex. jar cf wc.jar MapreduceTime*.class

Step 3. Run hadoop: 
hadoop jar wc.jar FILENAME TWEET DATA /user/YOUR-USERNAME/tweet/output
ex. hadoop jar wc.jar MapreduceTime /datasets/tweetInput/tweets2009-06.txt /user/ast-17s/tweet/output


Tweet files are located at these locations for usage. 
Replace the "TWEET DATA" with one of these locations:
/datasets/tweetInput/tweets2009-06.txt
/datasets/tweetInputLarge/tweets2009-11.txt
/datasets/tweetInputLarge/tweets2009-12.txt

You must either remove the files and the directory, or create a new directory for each test,
or else hadoop will throw an error.

To view your generated file upon success, use:
hadoop fs -cat /user/YOUR-USERNAME/tweet/output/part-r-00000
Or:
go to http://lenss-comp1.cse.tamu.edu:50070/explorer.html#/user/YOUR-USERNAME/ in your web browser
find your username, and go to the tweet/output file location

To remove files, use:
hadoop fs -rm /user/YOUR-USERNAME/tweet/output/part-r-00000
hadoop fs -rm /user/YOUR-USERNAME/tweet/output/_SUCCESS

To remove a directory, ensure it is empty, and then use:
hadoop fs -rmdir /user/YOUR-USERNAME/tweet/output/

