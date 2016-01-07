<?php

include __DIR__ . '/fixtures/Database.php';

use Pgdbsync\DbConn;

class ContainerBuilderTest extends \PHPUnit_Framework_TestCase
{
    private $database;
    private $conf;

    public function __construct()
    {
        $this->conf = parse_ini_file(__DIR__ . "/fixtures/conf.ini", true);
    }

    public function test_compare_same_squema()
    {
        $dbVc = new Pgdbsync\Db();
        $dbVc->setMasrer(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel']));

        $this->assertEquals("Already sync : gonzalo1\n", $dbVc->diff('public'));
    }

    public function test_compare_different_databases()
    {
        $dbVc = new Pgdbsync\Db();
        $dbVc->setMasrer(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel2']));

        $expected = "DBNAME : gonzalo2\n-----------------\n\nDROP TABLE public.testtable2;";
        $this->assertEquals($expected, trim($dbVc->diff('public')));
    }

    public function setUp()
    {
        $this->database = new Database($this->conf);
        $this->database->executeInDatabase('devel', function (PDO $conn) {
            $conn->exec("CREATE TABLE testTable (
                userid VARCHAR PRIMARY KEY  NOT NULL ,
                password VARCHAR NOT NULL ,
                name VARCHAR,
                surname VARCHAR
            );");
        });

        $this->database->executeInDatabase('devel2', function (PDO $conn) {
            $conn->exec("CREATE TABLE testTable (
                userid VARCHAR PRIMARY KEY  NOT NULL ,
                password VARCHAR NOT NULL ,
                name VARCHAR,
                surname VARCHAR
            );");
            $conn->exec("CREATE TABLE testTable2 (
                userid VARCHAR PRIMARY KEY  NOT NULL ,
                password VARCHAR NOT NULL ,
                name VARCHAR,
                surname VARCHAR
            );");
        });
    }

    public function tearDown()
    {
        $this->database->executeInDatabase('devel', function(PDO $conn) {
            $conn->exec("DROP TABLE testTable");
        });

        $this->database->executeInDatabase('devel2', function(PDO $conn) {
            $conn->exec("DROP TABLE testTable");
            $conn->exec("DROP TABLE testTable2");
        });
    }
}