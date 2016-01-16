<?php

include_once __DIR__ . '/fixtures/Database.php';

include_once __DIR__ . '/fixtures/Database.php';
include_once __DIR__ . '/fixtures/StringParser.php';

use Pgdbsync\DbConn;
use Pgdbsync\Db;

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
        $dbVc = new Db();
        $dbVc->setMaster(new DbConn($this->conf['devel']));
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

        $this->assertEquals(StringParser::trimLines($expected), StringParser::trimLines($diff[0]['diff'][0]));
    }

    public function setUp()
    {
        $this->database = new Database($this->conf);
        $this->database->executeInDatabase('devel', function (PDO $conn) {
            $user = $this->conf['devel2']['USER'];
            $conn->exec("CREATE TABLE testTable (
                userid VARCHAR PRIMARY KEY NOT NULL,
                password VARCHAR NOT NULL ,
                name VARCHAR,
                surname VARCHAR
            );");
            $conn->exec("GRANT ALL ON TABLE public.testtable TO {$user}");
        });
    }

    public function tearDown()
    {
        $this->database->executeInDatabase('devel', function(PDO $conn) {
            $conn->exec("DROP TABLE testTable");
        });
    }
}