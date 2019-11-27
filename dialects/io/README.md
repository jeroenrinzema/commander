# IO dialect

A in-memory dialect capable of reading and writing byte messages to a given `io.Reader`/`io.Writer`.
The dialect could be used in any library accepting these interfaces.
The messages are encoded and written as a UTF-8 string.