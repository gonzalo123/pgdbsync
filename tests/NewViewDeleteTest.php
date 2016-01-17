<?php

include_once __DIR__ . '/fixtures/Database.php';
include_once __DIR__ . '/fixtures/StringParser.php';

use Pgdbsync\DbConn;
use Pgdbsync\Db;

class NewViewDeleteTest extends \PHPUnit_Framework_TestCase
{
    private $database;
    private $conf;

    public function __construct()
    {
        $this->conf = parse_ini_file(__DIR__ . "/fixtures/conf.ini", true);
    }

    public function test_new_view()
    {
        $dbVc = new Db();
        $dbVc->setMaster(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel2']));

        $diff = $dbVc->raw('public');

        $this->assertCount(1, $diff);
        $this->assertCount(2, $diff[0]['diff']);

        $expected = "
            DROP VIEW public.myview;";

        $this->assertEquals(StringParser::trimLines($expected), StringParser::trimLines($diff[0]['diff'][0]));
    }

    public function setUp()
    {
        $this->database = new Database($this->conf);
        $this->database->executeInDatabase('devel2', function (PDO $conn) {
            $user = $this->conf['devel2']['USER'];
            $conn->exec("CREATE TABLE testTable (
                userid VARCHAR PRIMARY KEY NOT NULL,
                password VARCHAR NOT NULL ,
                name VARCHAR,
                surname VARCHAR
            );");
            $conn->exec("CREATE VIEW myView AS
                SELECT *
                FROM testTable
                WHERE surname = 'x';");
        });
    }

    public function tearDown()
    {
        $this->database->executeInDatabase('devel2', function(PDO $conn) {
            $conn->exec("DROP view myView");
            $conn->exec("DROP TABLE testTable");
        });
    }
}