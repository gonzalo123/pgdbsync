<?php

include_once __DIR__ . '/fixtures/Database.php';
include_once __DIR__ . '/fixtures/StringParser.php';

use Pgdbsync\DbConn;
use Pgdbsync\Db;

class FunctionTest extends \PHPUnit_Framework_TestCase
{
    private $database;
    private $conf;

    public function __construct()
    {
        $this->conf = parse_ini_file(__DIR__ . "/fixtures/conf.ini", true);
    }

    public function test_function()
    {
        $dbVc = new Db();
        $dbVc->setMaster(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel2']));

        $diff = $dbVc->raw('public');

        $this->assertCount(1, $diff);
        $this->assertCount(1, $diff[0]['diff']);

        $expected = "
            CREATE OR REPLACE FUNCTION public.hello(name character varying) RETURNS character varying
            LANGUAGE plpgsql AS \$function\$
            BEGIN
                RETURN 'Hello ' || name;
            END;
            \$function\$
        ";

        $this->assertEquals(StringParser::trimLines($expected), StringParser::trimLines($diff[0]['diff'][0]));
    }

    public function setUp()
    {
        $this->database = new Database($this->conf);
        $this->database->executeInDatabase('devel', function (PDO $conn) {
            $conn->exec("CREATE or replace FUNCTION hello(name varchar) RETURNS varchar AS $$
                BEGIN
                    RETURN 'Hello ' || name;
                END;
                $$ LANGUAGE plpgsql;");
        });
    }

    public function tearDown()
    {
        $this->database->executeInDatabase('devel', function(PDO $conn) {
            $conn->exec("DROP FUNCTION hello(name varchar)");
        });
    }
}