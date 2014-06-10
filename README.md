SimpleDHT
=========

Implementation of a Distributed Hash Table in android.

The key features of the implementation include:
1. Node Joins: When A node joins it's hash is generated and it is added to the chord in the appropriate position.
2. Hash Operation: SHA-1 algorithm was used to generate hash values.
3. Key-Value storage: A key-value storage was implemented at each node.
4. Insertions: The key-value pairs insertions are forwarded to the appropriate node.
5. Query: Local query as well as Global query methods implemented.
6. Deletions: Local as well as Global deletions mehtods implemented.
