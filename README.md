# pyorientdb_ops
Pyorient has very limited operations when it comes to executing commands on to the orientdb database Since the pyorient is only supported for Orient DB versions below 3.0, I decided to post my code up to create these commands and interfaces to make it easier to use orientdb through python.

I extend the class from pyorient - OrientDB class and create queries and executors so that we can use them seamlessly in code..

A lot of work needs to go in to separating query makers and executors. Especially in the Pythonic way. I have just made this module for easier access in creating Graphs through orient DB and in python as the standard pyorient library does not contain the same out-of-the-box

--- Requirements --- 

Python 2.7
OrientDB < 3.0

Currently, pyorient does not support OrientDB above the 3.0 version.
