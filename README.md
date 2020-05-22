# Apple Interview Question
This application is to explore Spark Windowing Functions
####QUESTION####

LOG DATA-->

ImpressionID,Ad_ID,UserID,timestamp
i1,A1,U1,10:00AM Jan 1
i2,A3,U1,10:45AM Jan 1
i3,A2,U1,10:50AM Jan 1
i4,A1,U1,11:00AM Jan 1

INPUT--->
A1,A2,A3

OutPut-->
Start:  10:45
End:  11:00
Duration:  15 Mins

The Smallest Window in which all the mentioned ads are displayed at least once

