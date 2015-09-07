# Environment Details

* A one-node HDP cluster is running on a server named namenode that is installed with various HDP components, including HDFS, MapReduce, YARN, Tez and Slider.

* You are currently logged in to an Ubuntu instance as a user named horton. As the horton user, you can SSH onto the cluster as the root user:

  ```
  $ ssh root@namenode
  ```
  
* The root password on the namenode is hadoop.

* Eclipse is installed and a shortcut is provided on the Desktop.

* A project named `Task1` is created for you, and a class named `task1.Task1` is stubbed out already. The build file for this project is preconfigured to use `task1.Task1` as the main class, and the project has the proper build path for developing Hadoop MapReduce applications.

* To build the project, right-click on the `Task1` project folder in Eclipse and select Run As -> Gradle Build.

* Ambari is available at http://namenode:8080. The username and password for Ambari are both admin.

# TASK 1

There are two folders in HDFS in the `/user/horton` folder: `flightdelays` and `weather`. These are comma-separated files that contain flight delay information for airports in the U.S. for the year 2008, along with the weather data from the San Francisco airport. Write and execute a Java MapReduce application that satisfies the following criteria:


1. Join the flight delay data in `flightdelays` with the weather data in `weather`. Join the data by the day, month and year and also where the "Dest" column in `flightdelays` is equal to "SFO".

2. The output of each delayed flight into SFO consists of the following fields:
  
  ```
  Year,Month,DayofMonth,DepTime,ArrTime,UniqueCarrier,FlightNum,
  ActualElapsedTime,ArrDelay,DepDelay,Origin,Dest,PRCP,TMAX,TMIN
  ```

  For example, for the date 2008-01-03, there is a delayed flight number 488 from Las Vegas (LAS) to San Francisco (SFO). The corresponding output would be:
  
  ```
  2008,1,3,1426,1605,WN,488,99,35,31,LAS,SFO,43,150,94
  ```

3. The output is sorted by date ascending, and on each day the output is sorted by ArrDelay descending (so that the longest arrival delays appear first).

4. The output is in text files in a new folder in HDFS named task1 with values separated by commas

5. The output is in two text files
