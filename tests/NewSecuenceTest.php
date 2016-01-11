<?php

include_once __DIR__ . '/fixtures/Database.php';

use Pgdbsync\DbConn;

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
        $dbVc = new Pgdbsync\Db();
        $dbVc->setMasrer(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel2']));

        $diff = $dbVc->raw('public');

        $this->assertCount(1, $diff);
        $this->assertCount(1, $diff[0]['diff']);

        $expected = "
CREATE SEQUENCE public.mysecuence
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1;";

        $this->assertEquals($expected, $diff[0]['diff'][0]);
    }

    public function setUp()
    {
        $this->database = new Database($this->conf);
        $this->database->executeInDatabase('devel', function (PDO $conn) {
            $user = $this->conf['devel2']['USER'];
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