<?php
namespace Pgdbsync;
class Sequence
{
    private $_pdo    = null;
    private $_name   = null;
    private $_owner  = null;
    private $_meta   = null;
    private $_acl    = null;
    private $_schema = null;

    function __construct(\PDO $pdo, $meta, $schema)
    {
        $this->_pdo   = $pdo;
        $this->_schema = $schema;
        $this->_name  = $meta['relname'];
        $this->_acl   = $meta['relacl'];
        $this->_owner = $meta['owner'];
        $this->_meta  = $this->getSequenceInfo($schema, $this->_name);
    }
    
    public function grants()
    {
    	$out = array();
    	//{user1=rwU/user1,user2=U/user1}
    	$acl = $this->_acl;
    	$acl = str_replace("{", null, $acl);
    	$acl = str_replace("}", null, $acl);
    	$aclArr = (array) explode(",", $acl);
    	foreach ($aclArr as $item) {
    		$x = explode("=", $item);
            if ($x[0] != '') {
        		$out[] = $x[0];
            }
    	}
        return $out;
    }
	private function getSequenceInfo($schema, $name)
	{
		$out = array();
		$stmt = $this->_pdo->prepare("SELECT * FROM {$this->_schema}.{$name}");
		$stmt->execute();	
		$out = $stmt->fetchAll();
		return $out[0];
	}
	
    public function getName()
    {
        return $this->_name;
    }
    
    public function getOwner()
    {
        return $this->_owner;
    }
    
    public function getIncrement()
    {
		return $this->_meta['increment_by'];
    }
    
    public function getMinValue()
    {
    	return $this->_meta['min_value'];
    }
    
    public function getMaxValue()
    {
    	$a = $this->_meta;
    	return $this->_meta['max_value'];
    }
    
    public function getStartValue()
    {
    	return $this->_meta['start_value'];
    }
}
