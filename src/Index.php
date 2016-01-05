<?php
namespace Pgdbsync;
class Index
{
    private $_pdo  = null;
    private $_meta = null;

    function __construct(\PDO &$pdo, $meta)
    {
        $this->_pdo  = $pdo;
        $this->_meta = $meta;
    }

}
