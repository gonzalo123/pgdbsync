<?php
include(__DIR__ . '/../vendor/autoload.php');

use Pgdbsync\DbConn;

$conf = parse_ini_file("conf.ini", true);

$dbVc = new Pgdbsync\Db();
$dbVc->setMaster(new DbConn($conf['devel']));
$dbVc->setSlave(new DbConn($conf['devel']));

print_r($dbVc->diff('public'));