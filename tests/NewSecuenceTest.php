<?php

include_once __DIR__ . '/fixtures/Database.php';
include_once __DIR__ . '/fixtures/StringParser.php';

use Pgdbsync\DbConn;
use Pgdbsync\Db;

class NewSecuenceTest extends \PHPUnit_Framework_TestCase
{
    private $database;
    private $conf;

    public function __construct()
    {
        $this->conf = parse_ini_file(__DIR__ . "/fixtures/conf.ini", true);
    }

    public function test_new_sequence()
    {
        $dbVc = new Db();
        $dbVc->setMaster(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel2']));

        $diff = $dbVc->raw('public');

        $this->assertCount(1, $diff);
        $this->assertCount(1, $diff[0]['diff']);

        $expected = "
            CREATE SEQUENCE public.mysecuence
              INCREMENT 1
              MINVALUE 1
              MAXVALUE 9223372036854775807
              START 342;";
        $this->assertEquals(StringParser::trimLines($expected), StringParser::trimLines($diff[0]['diff'][0]));
    }

    public function setUp()
    {
        $this->database = new Database($this->conf);
        $this->database->executeInDatabase('devel', function (PDO $conn) {
            $conn->exec("CREATE SEQUENCE mySecuence
              INCREMENT 1
              MINVALUE 1
              MAXVALUE 9223372036854775807
              START 342
              CACHE 1;");
        });
    }

    public function tearDown()
    {
        $this->database->executeInDatabase('devel', function(PDO $conn) {
            $conn->exec("DROP SEQUENCE mySecuence;");
        });
    }
}