<?php
namespace Pgdbsync;

class Column
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
        return '"' . $this->_meta['column_name'] . '"';
    }

    public function getType()
    {
        return $this->_meta['data_type'];
    }

    public function getOrder()
    {
        return $this->_meta['ordinal_position'];
    }


    public function getIsNullable()
    {
        return $this->_meta['is_nullable'] == 'YES' ? true : false;
    }

    public function getPrecision()
    {
        switch (strtolower($this->_meta['data_type'])) {
            case 'character varying':
                return $this->_meta['character_maximum_length'];
            case 'character':
                return $this->_meta['character_maximum_length'];
            case 'text':
                return null;
            case 'real':
            case 'integer':
                return null;
            case 'bigint':
                return null;
            case 'numeric':
                if ($this->_meta['numeric_precision']) {
                    return $this->_meta['numeric_precision'] . ', ' . $this->_meta['numeric_scale'];
                } else {
                    return null;
                }
            default:
                return null;
        }
    }
}

