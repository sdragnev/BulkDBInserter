# BulkDBInserter
Helper class that can buffer multiple PDO writes into one query.

Example usage, inserting 1200 records in the "accounts" table given an existing PDO connection object $pdo,
500 (default) records at a time:

```
// Inserts 1200 records in 3 queries (500 + 500 + 200)
$writer = new BulkInserter($pdo, "accounts", ["id", "name"]);
// $writer->setBatch(600); // optionally set the buffer size to a different number

for ($i = 0; $i < 1200; $i++) {
    $writer->write([$i, "John $i"]);
}

$writer->flush(true);   // flushes the remaining buffer
```