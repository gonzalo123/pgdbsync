<?php

include_once __DIR__ . '/fixtures/Database.php';

use Pgdbsync\DbConn;

class NewTableTest extends \PHPUnit_Framework_TestCase
{
    private $database;
    private $conf;

    public function __construct()
    {
        $this->conf = parse_ini_file(__DIR__ . "/fixtures/conf.ini", true);
    }

    public function test_new_table()
    {
        $dbVc = new Pgdbsync\Db();
        $dbVc->setMasrer(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel2']));

        $expected = "DBNAME : gonzalo2\n-----------------\n\nCREATE TABLE public.testtable(\n \"userid\" character varying NOT NULL,\n \"password\" character varying NOT NULL,\n \"name\" character varying,\n \"surname\" character varying,\n CONSTRAINT testtable_pkey PRIMARY KEY (\"userid\") \n);\nGRANT ALL ON TABLE public.testtable TO gonzalo;";
        $this->assertEquals($expected, trim($dbVc->diff('public')));
    }

    public function setUp()
    {
        $this->database = new Database($this->conf);
        $this->database->executeInDatabase('devel', function (PDO $conn) {
            $conn->exec("CREATE TABLE testTable (
                userid VARCHAR PRIMARY KEY NOT NULL,
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
    }
}