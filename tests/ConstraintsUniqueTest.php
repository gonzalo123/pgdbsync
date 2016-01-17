<?php

include_once __DIR__ . '/fixtures/Database.php';

include_once __DIR__ . '/fixtures/Database.php';
include_once __DIR__ . '/fixtures/StringParser.php';

use Pgdbsync\DbConn;
use Pgdbsync\Db;

class ConstraintsUniqueTest extends \PHPUnit_Framework_TestCase
{
    private $database;
    private $conf;

    public function __construct()
    {
        $this->conf = parse_ini_file(__DIR__ . "/fixtures/conf.ini", true);
    }

    public function test_constraints()
    {
        $dbVc = new Db();
        $dbVc->setMaster(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel2']));

        $diff = $dbVc->raw('public');

        $this->assertCount(1, $diff);
        $this->assertCount(1, $diff[0]['diff']);

        $expected = "
        CREATE TABLE public.products(
          \"product_no_unique\" integer,
          \"name\" text,
          \"price\" numeric,
          CONSTRAINT products_product_no_unique_key UNIQUE (\"product_no_unique\")
        );";

        $this->assertEquals(StringParser::trimLines($expected), StringParser::trimLines($diff[0]['diff'][0]));
    }

    public function setUp()
    {
        $this->database = new Database($this->conf);
        $this->database->executeInDatabase('devel', function (PDO $conn) {
            $conn->exec("CREATE TABLE products (
                product_no_unique integer UNIQUE,
                name text,
                price numeric
            );");
        });
    }

    public function tearDown()
    {
        $this->database->executeInDatabase('devel', function(PDO $conn) {
            $conn->exec("DROP TABLE products");
        });
    }
}