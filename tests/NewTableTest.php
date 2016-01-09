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

        $user = $this->conf['devel2']['USER'];
        $diff = $dbVc->raw('public');

        $this->assertCount(1, $diff);
        $this->assertCount(1, $diff[0]['diff']);


        $expected = "
CREATE TABLE public.testtable(
 \"userid\" character varying NOT NULL,
 \"password\" character varying NOT NULL,
 \"name\" character varying,
 \"surname\" character varying,
 CONSTRAINT testtable_pkey PRIMARY KEY (\"userid\")
);
GRANT ALL ON TABLE public.testtable TO {$user};";

        $this->assertEquals($expected, $diff[0]['diff'][0]);
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