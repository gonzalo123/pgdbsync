<?php
namespace Pgdbsync;
class Constraint
{
    private $_pdo  = null;
    private $_meta = null;

    function __construct(\PDO $pdo, $meta)
    {
        $this->_pdo  = $pdo;
        $this->_meta = $meta;
    }

    public function getName()
    {
        return $this->_meta['constraint_name'];
    }

    public function getConstraint()
    {
        return $this->_meta['consrc'];
    }
    
    public function getType()
    {
        return $this->_meta['constraint_type'];
    }
    
    public function getColumns()
    {
    	$columns = array();
    	$conkey = $this->_meta['conkey'];
    	if (strlen($conkey) > 0) { 
    		$conkey = str_replace("{", null, $conkey);
    		$conkey = str_replace("}", null, $conkey);
    		$columns = explode(",", $conkey);
    	}
    	return $columns;
    }
}

