<?php
namespace Pgdbsync;
class Table
{
    private $_pdo    = null;
    private $_schema = null;
    private $_tablename = null;
    private $_owner = null;
    private $_tablespace = null;

    function __construct(\PDO &$pdo, $schema, $tablename, $owner, $tablespace, $hasindexes, $dsn)
    {
        $this->_pdo = $pdo;
        $this->_schema     = $schema;
        $this->_tablename  = $tablename;
        $this->_owner      = $owner;
        $this->_tablespace = $tablespace;
        $this->_dsn = $dsn;
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

    const SQL_GET_GRANTS = "
    select distinct grantee 
		from information_schema.table_privileges  
	where 
	    table_schema = :SCHEMA and 
		table_name = :TABLE
    ";
    
    public function grants()
    {
    	$pdo = $this->_pdo;
        $out = array();
			
		$stmt = $this->_pdo->prepare(self::SQL_GET_GRANTS);
        $stmt->execute(array(
        	'SCHEMA' => $this->_schema,
        	'TABLE'  => $this->_tablename 
        	));	

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
        $out = array();
			
		$stmt = $this->_pdo->prepare(self::SQL_GET_COLUMNS);
        $stmt->execute(array(
        	'SCHEMA' => $this->_schema,
        	'TABLE'  => $this->_tablename 
        	));	

        foreach ($stmt->fetchAll() as $row) {
        	$out[] = new Column($this->_pdo, $row);
        }
        return $out;
    }

    const SQL_GET_CONSTRAINTS = "
	    SELECT
			*
		FROM
			information_schema.table_constraints a,
			pg_constraint b
		WHERE
			a.constraint_name = b.conname and
			table_schema = :SCHEMA and 
			table_name = :TABLE";

    public function constraints()
    {
    	$out = array();
        $stmt = $this->_pdo->prepare(self::SQL_GET_CONSTRAINTS);
        $stmt->execute(array(
        	'SCHEMA' => $this->_schema,
        	'TABLE'  => $this->_tablename 
        	));
		
        while ($row = $stmt->fetch()) {
        	$out[] = new Constraint($this->_pdo, $row);
        }
        return $out;
    }
}
