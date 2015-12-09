# How to update the TTL on multiple records

##Problem
You want to update the TTL on several records in an Aerospike Database. The records could be:
- All records in a Namespace
- All records in a Set
- All records the meet the criteria of a query

##Solution
The solution is to perform a query and iterate through the RecordSet. For each element, use the Digest of the primary key to Touch each record with the new TTL.

The source code for this solution is available on GitHub, and the README.md 
http://github.com/some place. 

