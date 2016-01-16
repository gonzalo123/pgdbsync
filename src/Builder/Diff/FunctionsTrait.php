<?php

namespace Pgdbsync\Builder\Diff;

trait FunctionsTrait
{
    protected $schema;
    protected $diff;
    protected $summary;
    protected $master;
    protected $slave;

    protected function diffFunctions()
    {
        $masterFunctions = isset($this->master['functions']) ? array_keys((array)$this->master['functions']) : [];
        $slaveFunctions  = isset($this->slave['functions']) ? array_keys((array)$this->slave['functions']) : [];

        // delete deleted functions
        $deletedFunctions = array_diff($slaveFunctions, $masterFunctions);
        if (count($deletedFunctions) > 0) {
            $this->deleteFunctions($deletedFunctions);
        }
        // create new functions
        $newFunctions = array_diff($masterFunctions, $slaveFunctions);

        // check differences
        foreach ($masterFunctions as $functionName) {
            if (!in_array($functionName, $newFunctions)) {
                $definitionMaster = $this->master['functions'][$functionName]['definition'];
                $definitionSlave  = $this->slave['functions'][$functionName]['definition'];

                if (md5($definitionMaster) != md5($definitionSlave)) {
                    $newFunctions[] = $functionName;
                }
            }
        }

        if (count($newFunctions) > 0) {
            $this->createFunctions($newFunctions);
        }
    }

    protected function deleteFunctions($functions)
    {
        if (count((array)$functions) > 0) {
            foreach ($functions as $function) {
                $this->diff[]                        = "DROP FUNCTION {$function};";
                $this->summary['function']['drop'][] = "{$function}";
            }
        }
    }

    protected function createFunctions($functions)
    {
        if (count((array)$functions) > 0) {
            foreach ($functions as $function) {
                $buffer                                = $this->master['functions'][$function]['definition'];
                $this->summary['function']['create'][] = "{$this->schema}.{$function}";
                $this->diff[]                          = $buffer;
            }
        }
    }
}