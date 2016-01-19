<?php
namespace Pgdbsync;

class Functiondb
{
    private $_pdo        = null;
    private $_meta       = null;
    private $_schema     = null;
    private $_definition = null;

    function __construct(\PDO $pdo, $meta, $schema)
    {
        $this->_pdo        = $pdo;
        $this->_schema     = $schema;
        $this->_meta       = $meta;
        $oid               = $meta['oid'];
        $this->_definition = $this->getFunctionInfo($oid);
    }

    private function getFunctionInfo($oid)
    {
        $stmt = $this->_pdo->prepare("select pg_get_functiondef({$oid})");
        $stmt->execute();
        $out = $stmt->fetchAll();

        return $out[0][0];
    }

    public function getName()
    {
        $types       = null;
        $proargtypes = (array)explode(" ", $this->_meta['proargtypes']);

        if (count($proargtypes) > 0) {
            $typeNameArr = [];

            $stmt = $this->_pdo->prepare("select typname from pg_type where oid = :OID");
            foreach ($proargtypes as $type) {
                if (!empty($type)) {
                    $stmt->execute(['OID' => $type]);
                    $out           = $stmt->fetchAll();
                    $typeNameArr[] = $out[0][0];
                }
            }
            $types = implode(", ", $typeNameArr);
        }

        $name = $this->_meta['nspname'] . '.' . $this->_meta['proname'] . "({$types})";

        return $name;
    }

    public function getDefinition()
    {
        return $this->_definition;
    }
}
