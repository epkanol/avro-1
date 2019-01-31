# Avro Java <-> Python compatibility tests

## Status
There are several issues with the Avro Python implementation, for instance

https://stackoverflow.com/questions/48442534/how-do-you-serialize-a-union-field-in-avro-using-python-when-attributes-match
https://issues.apache.org/jira/browse/AVRO-1968
https://issues.apache.org/jira/browse/AVRO-1343
https://issues.apache.org/jira/browse/AVRO-973
https://issues.apache.org/jira/browse/AVRO-283

This branch is an attempt to make a better, value-based, implementation of avro serialization and deserializtion to/from Python, without losing any type information.

The goal is that a blob encoded by Java, no matter what features were used, should:
1) be able to be decoded up to Python objects,
2) Python code does arbitrary transformations allowed by the schema (could be identity transformation, i.e. no change), and
3) Python object structure is encoded back to a blob which, when parsed by a Java reader, would not contain unintended type transformations.

The current Python implementation fails this test, as unions are always encoded by the last matching branch of the union.
The fastavro project has chosen the opposite strategy, where the first matching branch is used.
Neither is satisfactory, as illustrated by:
Example:
Consider a union [A,B,C], where A, B, and C are all compatible (for example int, long, bool, or more complex record-based structures).
A Java writer, encoding a B value in said union, would be read as an A by fastavro, and C by the avro-python3 reader. This change would then be persisted when the union is serialized back to a blob, in effect transforming the type of stored value in the union.

Thus, the type information encoded in the binary union data is lost.

## Test cases

1. Start by compiling and running the test cases:
        lang/java/ipc/src/test/java/org/apache/avro/WriteInteropData.java
        lang/java/ipc/src/test/java/org/apache/avro/WriteUnionsData.java
2. These will generate some directories, and data in /tmp
3. Reader test cases will fail at this step, as there are no python-generated data yet
4. Run the following python tests, which corresponds to the java-based above:
        lang/py3/avro/tests/test_datafile_interop.py
        lang/py3/avro/tests/test_datafile_unions.py
5. These tests will read the Java-generated data files, and create avro-python-3-generated files
6. If not already done, clone the fastavro project from my github account, and switch to the eavro branch:
        $ git clone https://github.com/epkanol/fastavro.git
        $ git checkout eavro
7. Run the corresponding test case from fastavro:
    tests/test_datafile.py
8. Now all needed data files should be available, under /tmp/java, /tmp/python, /tmp/fastavro
9. Go back to the original java classes, and rerun the test cases:
        lang/java/ipc/src/test/java/org/apache/avro/WriteInteropData.java
        lang/java/ipc/src/test/java/org/apache/avro/WriteUnionsData.java

10. Inspect the failing ones.
11. Some errors from the pyavro tests are "hard", meaning that they affect the data that was actually set in the test case, while others are "soft", meaning that they are more general (common for all test cases)
12. Repeat as needed, due to the changes you make.

## Future directions

1. Remove the cyclic dependency java/python
2. Remove hard dependency on /tmp
3. Extend with other types to test
