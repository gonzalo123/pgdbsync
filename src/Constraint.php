<?php
namespace Pgdbsync;

class Constraint
{

    private $_pdo = null;

    private $_meta = null;

    public static $ON_ACTION_MAP = array(
        'a' => 'no action',
        'r' => 'restrict',
        'c' => 'cascade',
        'n' => 'set null',
        'd' => 'set default'
    );
    
    public static $MATCH_MAP = array(
        'f' => 'full',
        'p' => 'partial',
        'u' => 'simple'
    );

    function __construct(\PDO $pdo, $meta)
    {
        $this->_pdo = $pdo;
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

    public function getOnDeleteOption()
    {
        return $this->_meta['confdeltype'];
    }

    public function getOnUpdateOption()
    {
        return $this->_meta['confupdtype'];
    }
    
    public function getMatchOption()
    {
        return $this->_meta['confmatchtype'];
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

    public function getReftable()
    {
        return $this->_meta['relname'];
    }

    public function getRefcolumn()
    {
        return $this->_meta['attname'];
    }
}

