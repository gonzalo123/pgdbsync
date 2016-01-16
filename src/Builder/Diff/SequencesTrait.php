<?php

namespace Pgdbsync\Builder\Diff;

trait SequencesTrait
{
    protected function diffSequences()
    {
        $masterSequences = isset($this->master['sequences']) ? array_keys((array)$this->master['sequences']) : [];
        $slaveSequences  = isset($this->slave['sequences']) ? array_keys((array)$this->slave['sequences']) : [];

        // delete deleted sequences
        $deletedSequences = array_diff($slaveSequences, $masterSequences);
        if (count($deletedSequences) > 0) {
            $this->deleteSequences($deletedSequences);
        }
        // create new sequences
        $newSequences = array_diff($masterSequences, $slaveSequences);
        if (count($newSequences) > 0) {
            $this->createSequences($newSequences);
        }
    }

    protected function deleteSequences($sequences)
    {
        if (count((array)$sequences) > 0) {
            foreach ($sequences as $sequence) {
                $this->diff[]                        = "DROP SEQUENCE {$this->schema}.{$sequence};";
                $this->summary['sequence']['drop'][] = "{$this->schema}.{$sequence}";
            }
        }
    }

    protected function createSequences($sequences)
    {
        if (count((array)$sequences) > 0) {
            foreach ($sequences as $sequence) {
                $this->createSequence($sequence);
            }
        }
    }

    protected function createSequence($sequence)
    {
        $increment = $this->master['sequences'][$sequence]['increment'];
        $minvalue  = $this->master['sequences'][$sequence]['minvalue'];
        $maxvalue  = $this->master['sequences'][$sequence]['maxvalue'];
        $start     = $this->master['sequences'][$sequence]['startvalue'];

        $owner  = $this->master['sequences'][$sequence]['owner'];
        $buffer = "\nCREATE SEQUENCE {$this->schema}.{$sequence}";
        $buffer .= "\n  INCREMENT {$increment}";
        $buffer .= "\n  MINVALUE {$minvalue}";
        $buffer .= "\n  MAXVALUE {$maxvalue}";
        $buffer .= "\n  START {$start};";
        if ($this->settings['alter_owner'] === true) {
            $buffer .= "\nALTER TABLE {$this->schema}.{$sequence} OWNER TO {$owner};";
        }
        foreach ($this->master['sequences'][$sequence]['grants'] as $grant) {
            if (!empty($grant)) {
                $buffer .= "\nGRANT ALL ON TABLE {$this->schema}.{$sequence} TO {$grant};";
            }
        }
        $this->diff[]                          = $buffer;
        $this->summary['secuence']['create'][] = "{$this->schema}.{$sequence}";
    }

}