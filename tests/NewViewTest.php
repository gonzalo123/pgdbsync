<?php

include_once __DIR__ . '/fixtures/Database.php';

use Pgdbsync\DbConn;

class NewViewTest extends \PHPUnit_Framework_TestCase
{
    private $database;
    private $conf;

    public function __construct()
    {
        $this->conf = parse_ini_file(__DIR__ . "/fixtures/conf.ini", true);
    }

    public function test_new_view()
    {
        $dbVc = new Pgdbsync\Db();
        $dbVc->setMasrer(new DbConn($this->conf['devel']));
        $dbVc->setSlave(new DbConn($this->conf['devel2']));

        $user = $this->conf['devel2']['USER'];
        $diff = $dbVc->raw('public');

        $this->assertCount(1, $diff);
        $this->assertCount(2, $diff[0]['diff']);


        $expected = "
CREATE OR REPLACE VIEW public.myview AS
   SELECT testtable.userid,
    testtable.password,
    testtable.name,
    testtable.surname
   FROM testtable
  WHERE ((testtable.surname)::text = 'x'::text);;
GRANT ALL ON TABLE public.myview TO {$user};";

        $this->assertEquals(trim($expected), trim($diff[0]['diff'][0]));
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
            $conn->exec("CREATE VIEW myView AS
                SELECT *
                FROM testTable
                WHERE surname = 'x';");
            $conn->exec("GRANT ALL ON TABLE public.testtable TO {$user}");
        });
    }

    public function tearDown()
    {
        $this->database->executeInDatabase('devel', function(PDO $conn) {
            $conn->exec("DROP view myView");
            $conn->exec("DROP TABLE testTable");
        });
    }
}