<?php
namespace Pgdbsync;

use Pgdbsync\Builder\Conf;
use Pgdbsync\Builder\Diff;

class Db
{
    private $schema;

    /** @var DbConn null  */
    private $masterDb = null;
    private $slaveDb = [];

    public function setMaster(DbConn $db)
    {
        $this->masterDb = $db;
    }

    public function setSlave(DbConn $db)
    {
        $this->slaveDb[] = $db;
    }

    public function summary($schema)
    {
        $this->schema = $schema;
        $buffer = [];
        $data   = $this->createDiff();
        foreach ($data as $row) {
            if (count($row['summary']) > 0) {
                $title    = "DBNAME : " . $row['db']->dbName();
                $buffer[] = $title;
                $buffer[] = str_repeat("-", strlen($title));

                foreach ($row['summary'] as $type => $info) {
                    $buffer[] = $type;
                    foreach ($info as $mode => $objects) {
                        foreach ($objects as $object) {
                            $buffer[] = " " . $mode . " :: " . $object;
                        }
                    }
                }
                $buffer[] = "\n";
            }
        }

        return implode("\n", $buffer) . "\n";
    }

    public function run($schema)
    {
        $this->schema = $schema;
        $errors = [];
        $data   = $this->createDiff();
        foreach ($data as $row) {
            /** @var DbConn $db */
            $db   = $row['db'];
            $host = $db->getDbHost() . " :: " . $db->getDbname();
            foreach ($row['diff'] as $item) {
                try {
                    $db->exec($item);
                } catch (\PDOException $e) {
                    $errors[$host][] = [
                        $item,
                        $e->getMessage()
                    ];
                }
            }
        }

        return $errors;
    }

    public function raw($schema)
    {
        $this->schema = $schema;
        return $this->createDiff();
    }

    public function diff($schema)
    {
        $this->schema = $schema;
        $buffer = [];
        $data   = $this->createDiff();
        foreach ($data as $row) {
            if (count($row['diff']) > 0) {
                $title    = "DBNAME : " . $row['db']->dbName();
                $buffer[] = $title;
                $buffer[] = str_repeat("-", strlen($title));

                foreach ($row['diff'] as $item) {
                    $buffer[] = $item;
                }
                $buffer[] = "\n";
            } else {
                $buffer[] = "Already sync : " . $row['db']->dbName();
            }
        }

        return implode("\n", $buffer) . "\n";
    }

    private function buildConf(DbConn $db)
    {
        return (new Conf($db))->build($this->schema);
    }

    private function createDiff()
    {
        $out    = [];
        $master = $this->buildConf($this->masterDb->connect(), $this->schema);
        foreach ($this->slaveDb as $slaveDb) {
            $out[] = $this->createDiffPerDb($slaveDb, $master, $out);
        }

        return $out;
    }

    private function createDiffPerDb(DbConn $slaveDb, $master, $out)
    {
        $slave = $this->buildConf($slaveDb->connect(), $this->schema);
        if (md5(serialize($master)) == md5(serialize($slave))) {
            // echo "[OK] <b>{$this->schema}</b> " . $slaveDb->dbName() . "<br/>";
            $out = [
                'db'      => $slaveDb,
                'diff'    => [],
                'summary' => []
            ];
        } else {
            $diff = new Diff($this->schema);
            $diffResult = $diff->getDiff($master, $slave);
            $diffResult['db']      = $slaveDb;

            $out = $diffResult;
        }

        return $out;
    }
}
