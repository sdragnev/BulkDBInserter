<?php
/**
 * User: stan
 * Created on: 2017-11-14
 *
 * Functionality to bulk-insert values into mysql using PDO.
 */

class BulkInserter
{

    /** @var  PDO */
    private $pdo;
    private $batch = 500; // how many rows to insert at once
    private $bufferSize;
    private $tableParams;
    private $tableInsertQuery;
    private $fields;
    private $totalCount;
    private $originalErrMode;
    public $writesRequested = 0;
    public $writesCompleted = 0;
    public $dieOnError = true;
    public $transaction;

    /**
     * BulkInserter constructor.
     * @param PDO $pdo PDO connector.
     * @param string $tableName Which table we're inserting into.
     * @param array $fieldNames An array of field names we'll be inserting.
     * @param bool $replace Whether to use REPLACE INTO instead of INSERT (only use if unique/primary keys are present)
     * @param bool $dieOnError If false, instead of throwing an exception, an error is echoed and the process will continue
     * @param bool $transaction Whether to use a transaction for writing.
     */
    public function __construct($pdo, $tableName, $fieldNames, $replace = false, $dieOnError = true, $transaction = true,
                                $totalCount = null) {
        $this->pdo = $pdo;
        $this->originalErrMode = $pdo->getAttribute(\PDO::ATTR_ERRMODE);    // save this so we can restore it
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $this->dieOnError = $dieOnError;
        $this->bufferSize = 0;
        $this->transaction = $transaction;
        $this->tableParams = [];
        $this->totalCount = $totalCount;
        $op = $replace ? "REPLACE" : "INSERT IGNORE";

        // We need to backtick table field names since the number (#) character throws a syntax error (thanks cmsp7000!)
        array_walk($fieldNames, function (&$field, $key) {
            $field = '`' . $field . '`';
        });
        $this->fields = '(' . implode(',', array_fill(0, count($fieldNames), '?')) . ')'; // eg (?, ?, ?)
        $this->tableInsertQuery = "$op INTO $tableName (" . implode(',', $fieldNames) . ') VALUES ';

        if ($this->transaction) {
            $result = $this->pdo->beginTransaction();
            if (!$result) {
                throw new \RuntimeException("Failed to start transaction in BulkInserter.\n" .
                    print_r($this->pdo->errorInfo(), 1));
            }
        }
    }

    /**
     * Queue up an insert.
     * @param $values
     */
    public function write($values) {
        array_push($this->tableParams, ...$values);
        $this->bufferSize++;

        // Flush the buffer
        if ($this->bufferSize == $this->batch) {
            $this->flush();
        } else if (!empty($this->totalCount) && ($this->writesCompleted + $this->bufferSize) == $this->totalCount) {
            // This is the final write and we know it
            $this->flush(true);
        }
    }

    /**
     * Execute multiple writes at once from an array of array of rows.
     * @param array $valuesArr
     */
    public function bulkWrite(array $valuesArr) {
        foreach ($valuesArr as $values) {
            $this->write($values);
        }
    }

    /**
     * Force a write.
     * @param $final  boolean Whether this will be the last operation for this object.
     */
    public function flush($final = false) {

        if ($this->bufferSize > 0) {
            // Only bother if we have anything to do
            $query = $this->tableInsertQuery . implode(',', array_fill(0, $this->bufferSize, $this->fields));

            try {
                $statement = $this->pdo->prepare($query);
                $statement->execute($this->tableParams);
            } catch (\PDOException $e) {
                fwrite(STDERR, sprintf("Error: %s\nTrace: %s\n", print_r($this->pdo->errorInfo(), 1),
                    $e->getTraceAsString()));
                if ($this->dieOnError) {
                    if ($this->pdo->inTransaction()) {
                        $this->pdo->rollBack();
                    }
                    throw $e;
                }
            }

            // We want these to be equal at the end (unless this is a REPLACE INTO)
            $this->writesRequested += $this->bufferSize;
            $this->writesCompleted += $statement->rowCount();

            // Clean the buffer
            $this->bufferSize = 0;
            $this->tableParams = [];
        }

        if ($final && $this->transaction) {
            try {
                $this->pdo->commit();
            } catch (\PDOException $e) {
                fwrite(STDERR, sprintf("Error: %s\nTrace: %s\n", print_r($this->pdo->errorInfo(), 1),
                    $e->getTraceAsString()));
                if ($this->dieOnError) {
                    if ($this->pdo->inTransaction()) {
                        $this->pdo->rollBack();
                    }
                    throw $e;
                }
            }

            $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, $this->originalErrMode);
        }

    }

    /**
     * Iterates over a data result and inserts each row into the table.
     * @param $result PDOStatement
     */
    public function insertPDOResult($result) {

        while ($row = $result->fetch(PDO::FETCH_NUM)) {
            $this->write($row);
        }
        $this->flush(true); // flush the write buffers
    }

    /**
     * Sets the number of writes done at once.
     * @param $num
     */
    public function setBatch($num) {
        $this->batch = $num;
    }
}
