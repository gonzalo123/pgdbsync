<?php

class Database
{
    private $conn;
    private $conf;

    public function __construct($conf)
    {
        $this->conf = $conf;
    }

    public function initDatabase($key, callable $callback)
    {
        $conn = new \PDO($this->getDsn($key), $this->conf[$key]['USER'], $this->conf[$key]['PASSWORD']);
        $conn->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $callback($conn);
        $this->conn[$key] = $conn;
    }

    public function executeInDatabase($key, callable $callback)
    {
        if (!isset($this->conn[$key])) {
            $this->connectToDatabase($key);
        }

        $callback($this->conn[$key]);
    }

    private function connectToDatabase($key)
    {
        $conn = new \PDO($this->getDsn($key), $this->conf[$key]['USER'], $this->conf[$key]['PASSWORD']);
        $conn->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $this->conn[$key] = $conn;
    }

    private function getDsn($key)
    {
        $dsnOptionsArr = [];
        if (isset($this->conf[$key]['HOST'])) {
            $dsnOptionsArr[] = "host={$this->conf[$key]['HOST']}";
        }
        if (isset($this->conf[$key]['DBNAME'])) {
            $dsnOptionsArr[] = "dbname={$this->conf[$key]['DBNAME']}";
        }
        if (isset($this->conf[$key]['CHARTSET'])) {
            $dsnOptionsArr[] = "charset={$this->conf[$key]['CHARTSET']}";
        }
        if (isset($this->conf[$key]['PORT'])) {
            $dsnOptionsArr[] = "port={$this->conf[$key]['PORT']}";
        }

        return "{$this->conf[$key]['TYPE']}:" . implode(';', $dsnOptionsArr);
    }
}