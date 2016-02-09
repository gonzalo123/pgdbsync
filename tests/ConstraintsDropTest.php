<?php

include_once __DIR__ . '/fixtures/Database.php';

include_once __DIR__ . '/fixtures/Database.php';
include_once __DIR__ . '/fixtures/StringParser.php';

use Pgdbsync\DbConn;
use Pgdbsync\Db;

class ConstraintsDropTest extends  \PHPUnit_Framework_TestCase
{
    private $database;
    private $conf;

    public function __construct()
    {
        $this->conf = parse_ini_file(__DIR__ . "/fixtures/conf.ini", true);
    }

    public function test_drop_constraints()
    {
        $dbVc = new Db();
        $dbVc->setMaster(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel2']));

        $diff = $dbVc->raw('public');

        $this->assertCount(1, $diff);
        $this->assertCount(1, $diff[0]['diff']);

        $expected = "ALTER TABLE public.products DROP CONSTRAINT \"products_price_check\" CASCADE;";

        $this->assertEquals(StringParser::trimLines($expected), StringParser::trimLines($diff[0]['diff'][0]));
    }

    public function setUp()
    {
        $this->database = new Database($this->conf);
        $this->database->executeInDatabase('devel', function (PDO $conn) {
            $conn->exec("CREATE TABLE products (
                product_no NUMERIC(3, 0),
                name text,
                price numeric(3, 0)
            );");
        });
        $this->database->executeInDatabase('devel2', function (PDO $conn) {
            $conn->exec("CREATE TABLE products (
                product_no NUMERIC(3, 0),
                name text,
                price numeric(3, 0) CHECK (price > 0)
            );");
        });
    }

    public function tearDown()
    {
        $this->database->executeInDatabase('devel', function(PDO $conn) {
            $conn->exec("DROP TABLE products");
        });

        $this->database->executeInDatabase('devel2', function(PDO $conn) {
            $conn->exec("DROP TABLE products");
        });
    }
}