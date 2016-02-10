<?php

include_once __DIR__ . '/fixtures/Database.php';

include_once __DIR__ . '/fixtures/Database.php';
include_once __DIR__ . '/fixtures/StringParser.php';

use Pgdbsync\DbConn;
use Pgdbsync\Db;

class ForeingKeyTest extends \PHPUnit_Framework_TestCase
{
    private $database;
    private $conf;

    public function __construct()
    {
        $this->conf = parse_ini_file(__DIR__ . "/fixtures/conf.ini", true);
    }

    public function test_foreing_keys()
    {
        $dbVc = new Db();
        $dbVc->setMaster(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel2']));

        $diff = $dbVc->raw('public');

        $this->assertCount(1, $diff);
        $this->assertCount(1, $diff[0]['diff']);

        $expected = "ALTER TABLE public.weather ADD CONSTRAINT \"weather_city_fkey\" FOREIGN KEY (\"city\") REFERENCES public.cities (city) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION;";

        $this->assertEquals(StringParser::trimLines($expected), StringParser::trimLines($diff[0]['diff'][0]));
    }

    public function setUp()
    {
        $this->database = new Database($this->conf);
        $this->database->executeInDatabase('devel', function (PDO $conn) {
            $conn->exec("CREATE TABLE cities (
                city     varchar(80) primary key,
                location point
                );");

            $conn->exec("CREATE TABLE weather (
                city      varchar(80) references cities(city),
                temp_lo   int,
                temp_hi   int,
                prcp      real,
                date      date
                );");

        });
        $this->database->executeInDatabase('devel2', function (PDO $conn) {
            $conn->exec("CREATE TABLE cities (
                city     varchar(80) primary key,
                location point
                );");

            $conn->exec("CREATE TABLE weather (
                city      varchar(80),
                temp_lo   int,
                temp_hi   int,
                prcp      real,
                date      date
                );");
        });
    }

    public function tearDown()
    {
        $this->database->executeInDatabase('devel', function(PDO $conn) {
            $conn->exec("DROP TABLE weather");
            $conn->exec("DROP TABLE cities");
        });

        $this->database->executeInDatabase('devel2', function(PDO $conn) {
            $conn->exec("DROP TABLE weather");
            $conn->exec("DROP TABLE cities");
        });
    }
}