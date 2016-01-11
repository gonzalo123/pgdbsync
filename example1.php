<?php
include("lib/Pgdbsync/includes.php");
use \Pgdbsync;
$conf  = parse_ini_file("conf.ini", true);

$dbVc = new Pgdbsync\Db();
$dbVc->setMaster(new Pgdbsync\DbConn($conf['development']));
$dbVc->setSlave(new Pgdbsync\DbConn($conf['production']));

echo "<pre>";
print_r($dbVc->diff('web'));
echo "</pre>";

