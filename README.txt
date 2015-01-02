GROUP MEMBERS

1. Sakthivel Manikam Arunachalam (UFID: 81118155)

2. Aravind Sunderrajan (UFID: 94543151)


HOW TO RUN

Directory structure to run command:

Project folder/
|
|---Project2.scala
|
|---SCALA-2.11.2/
|       ---------bin/
|               -------application.conf
|       ---------lib/
|---AKKA/
|       ---------*.jars(akka jars)


Compile command:

./scalac -cp "../../akka/*:../../scala-library-2.11.2.jar:." ../../project2.scala

Run command:

./scala -cp "../../akka/*:../../scala-library-2.11.2.jar:." project2 [numberofactors]  [topology] [algorithm]

Example: 

./scala -cp "../../akka/*:../../scala-library-2.11.2.jar:." project2 8000 full gossip



What is working:

*Both algorithms Gossip and Push-sum for all four topologies are converging.

*For detailed analysis of run times and trends please refer the Report.pdf.

Largest Network achieved:

Gossip:

Full-50000
Line-20000
2D - 50000
Imperfect 2D - 50000

Push-Sum:

Full-20000
Line-20000
2D - 20000
Imperfect 2D - 20000
