<?php
/**
 * Created on: 2017-11-14
 *
 * Functionality to bulk-insert values into mysql using PDO.
 */

class BulkInserter {

  /** @var  \PDO */
  private $pdo;
  private $batch = 500; // how many rows to insert at once
  private $bufferSize;
  private $tableParams;
  private $tableName;
  private $tableInsertQuery;
  private $fields;
  public $writesRequested = 0;
  public $writesCompleted = 0;
  public $dieOnError = true;

  /**
   * BulkInserter constructor.
   * @param $pdo  \PDO
   * @param $tableName  string  Which table we're inserting in.
   * @param $fieldNames array   An array of fields we'll be inserting.
   * @param $replace  bool      Whether to use REPLACE INTO instead of INSERT (only use if unique/primary keys are present)
   * @param $dieOnError  bool   If false, instead of throwing an exception, an error is echoed and the process will continue
   */
  public function __construct($pdo, $tableName, $fieldNames, $replace = false, $dieOnError = true) {
    $this->pdo = $pdo;
    $this->tableName = $tableName;
    $this->dieOnError = $dieOnError;
    $this->bufferSize = 0;
    $this->tableParams = [];
    $op = $replace ? "REPLACE" : "INSERT IGNORE";

    // We need to backtick table field names since the number (#) character throws a syntax error (thanks cmsp7000!)
    array_walk($fieldNames, function(&$field, $key) {
      $field = '`' . $field . '`';
    });
    $this->fields = '(' . implode(',', array_fill(0, count($fieldNames), '?')) . ')'; // eg (?, ?, ?)
    $this->tableInsertQuery = "$op INTO $tableName (" . implode(',', $fieldNames) . ') VALUES ';

    // Useless for MyISAM but potentially a time-saver for other engines
    $this->pdo->beginTransaction();
  }

  /**
   * Queue up an insert.
   * @param $values
   */
  public function write($values) {
    $this->tableParams = array_merge($this->tableParams, $values);
    $this->bufferSize++;

    // Flush the buffer
    if ($this->bufferSize == $this->batch) {
      $this->flush();
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
      $statement = $this->pdo->prepare($query);
      $result = $statement->execute($this->tableParams);

      if ($result === false) {
        if ($this->dieOnError) {
          $this->pdo->rollBack();
          throw new \RuntimeException($statement->errorInfo()[2]);
        } else {
          echo $statement->errorInfo()[2] . "\n";
        }
      }

      // We want these to be equal at the end (unless this is a REPLACE INTO)
      $this->writesRequested += $this->bufferSize;
      $this->writesCompleted += $statement->rowCount();

      // Clean the buffer
      $this->bufferSize = 0;
      $this->tableParams = [];
    }

    if ($final) {
      $this->pdo->commit();
    }
  }

  /**
   * Iterates over a data result and inserts each row into the table.
   * @param $result \PDOStatement
   */
  public function insertPDOResult($result) {

    while ($row = $result->fetch(\PDO::FETCH_NUM)) {
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
