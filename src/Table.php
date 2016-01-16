<?php
namespace Pgdbsync;

class Table
{
    private $_pdo        = null;
    private $_schema     = null;
    private $_tablename  = null;
    private $_owner      = null;
    private $_tablespace = null;
    private $_oid        = null;

    function __construct(\PDO $pdo, $schema, $tablename, $owner, $tablespace, $hasindexes, $oid)
    {
        $this->_pdo        = $pdo;
        $this->_schema     = $schema;
        $this->_tablename  = $tablename;
        $this->_owner      = $owner;
        $this->_tablespace = $tablespace;
        $this->_oid        = $oid;
    }

    public function getOwner()
    {
        return $this->_owner;
    }

    public function getTablespace()
    {
        return $this->_tablespace;
    }

    public function getName()
    {
        return $this->_tablename;
    }

    public function getOid()
    {
        return $this->_oid;
    }

    const SQL_GET_GRANTS = "
    select distinct grantee 
		from information_schema.table_privileges  
	where 
	    table_schema = :SCHEMA and 
		table_name = :TABLE
    ";

    public function grants()
    {
        $out = [];

        $stmt = $this->_pdo->prepare(self::SQL_GET_GRANTS);
        $stmt->execute([
            'SCHEMA' => $this->_schema,
            'TABLE'  => $this->_tablename
        ]);

        foreach ($stmt->fetchAll() as $row) {
            $out[] = $row[0];
        }

        return $out;
    }

    const SQL_GET_COLUMNS = "
	    SELECT
			*
		FROM
			information_schema.columns a
		WHERE
			table_schema = :SCHEMA and 
			table_name = :TABLE
		ORDER BY
			ordinal_position";

    public function columns()
    {
        $pdo = $this->_pdo;
        $out = [];

        $stmt = $this->_pdo->prepare(self::SQL_GET_COLUMNS);
        $stmt->execute([
            'SCHEMA' => $this->_schema,
            'TABLE'  => $this->_tablename
        ]);

        foreach ($stmt->fetchAll() as $row) {
            $out[] = new Column($this->_pdo, $row);
        }

        return $out;
    }

    const SQL_GET_CONSTRAINTS = "
	    SELECT
		    DISTINCT ON (a.constraint_name, a.table_name, b.confrelid, b.conname, b.conrelid, b.conkey, b.confkey, d.attname) 
            *
		FROM
			information_schema.table_constraints a,
			pg_constraint b
        LEFT JOIN pg_class AS c 
         	ON (b.confrelid = c.relfilenode)
        
         LEFT JOIN pg_attribute AS d
        	 ON (
         		c.relfilenode = d.attrelid AND
         		d.attnum = ANY (b.confkey)
         	)
		WHERE
			a.constraint_name = b.conname 
			AND table_schema = :SCHEMA 
			AND table_name = :TABLE 
            AND b.conrelid = (:SCHEMA || '.' || a.table_name)::regclass::oid   
            ";

    public function constraints()
    {
        $out  = [];
        $stmt = $this->_pdo->prepare(self::SQL_GET_CONSTRAINTS);
        $stmt->execute([
            'SCHEMA' => $this->_schema,
            'TABLE'  => $this->_tablename
        ]);

        while ($row = $stmt->fetch()) {
            $out[] = new Constraint($this->_pdo, $row);
        }

        return $out;
    }

    public static function getFkReferenceData()
    {
        return [
            'schema',
            'table',
            'columns'
        ];
    }

}
