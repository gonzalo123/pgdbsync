<?php
namespace Pgdbsync;

class View
{
    private $_pdo    = null;
    private $_meta   = null;
    private $_schema = null;

    function __construct(\PDO $pdo, $meta, $schema)
    {
        $this->_pdo    = $pdo;
        $this->_meta   = $meta;
        $this->_schema = $schema;
    }

    const SQL_GET_GRANTS = "
    SELECT DISTINCT grantee
		FROM information_schema.table_privileges
	WHERE
	    table_schema = :SCHEMA AND
		table_name = :TABLE
    ";

    public function grants()
    {
        $out = [];

        $stmt = $this->_pdo->prepare(self::SQL_GET_GRANTS);
        $stmt->execute([
            'SCHEMA' => $this->_schema,
            'TABLE'  => $this->_meta['viewname']
        ]);

        foreach ($stmt->fetchAll() as $row) {
            $out[] = $row[0];
        }

        return $out;
    }

    public function getName()
    {
        return $this->_meta['viewname'];
    }

    public function getOwner()
    {
        return $this->_meta['viewowner'];
    }

    public function getDefinition()
    {
        return $this->_meta['definition'];
    }
}
