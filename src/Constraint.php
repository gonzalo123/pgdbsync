<?php
namespace Pgdbsync;

class Constraint
{
    private $_pdo = null;

    private $_meta = null;

    public static $ON_ACTION_MAP = [
        'a' => 'no action',
        'r' => 'restrict',
        'c' => 'cascade',
        'n' => 'set null',
        'd' => 'set default'
    ];

    public static $MATCH_MAP = [
        'f' => 'full',
        'p' => 'partial',
        'u' => 'simple',
        's' => 'simple' // http://www.postgresql.org/message-id/CA+OCxowezVvmKW-6mhwaZ4KbJOAY9AbLJxY+vi1o2B6rpuhy6w@mail.gmail.com
    ];

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
        $columns = [];
        $conkey  = $this->_meta['conkey'];
        if (strlen($conkey) > 0) {
            $conkey  = str_replace("{", null, $conkey);
            $conkey  = str_replace("}", null, $conkey);
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

