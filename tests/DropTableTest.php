<?php

include_once __DIR__ . '/fixtures/Database.php';

use Pgdbsync\DbConn;
use Pgdbsync\Db;

class DropTableTest extends \PHPUnit_Framework_TestCase
{
    private $database;
    private $conf;

    public function __construct()
    {
        $this->conf = parse_ini_file(__DIR__ . "/fixtures/conf.ini", true);
    }

    public function test_drop_table()
    {
        $dbVc = new Db();
        $dbVc->setMaster(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel2']));

        $diff = $dbVc->raw('public');

        $this->assertCount(1, $diff);
        $this->assertCount(1, $diff[0]['diff']);
        $this->assertEquals("DROP TABLE public.testtable;", trim($diff[0]['diff'][0]));
    }

    public function setUp()
    {
        $this->database = new Database($this->conf);
        $this->database->executeInDatabase('devel2', function (PDO $conn) {
            $conn->exec("CREATE TABLE testTable (
                userid VARCHAR PRIMARY KEY NOT NULL,
                password VARCHAR NOT NULL ,
                name2 VARCHAR,
                surname VARCHAR
            );");
        });
    }

    public function tearDown()
    {
        $this->database->executeInDatabase('devel2', function(PDO $conn) {
            $conn->exec("DROP TABLE testTable");
        });
    }
}