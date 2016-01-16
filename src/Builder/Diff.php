<?php

namespace Pgdbsync\Builder;

use Pgdbsync\Builder\Diff\Functions;
use Pgdbsync\Builder\Diff\FunctionsTrait;
use Pgdbsync\Builder\Diff\SequencesTrait;
use Pgdbsync\Builder\Diff\ViewsTrait;
use Pgdbsync\Builder\Diff\TablesTrait;

class Diff
{
    use FunctionsTrait;
    use SequencesTrait;
    use ViewsTrait;
    use TablesTrait;

    // @todo extract this configuration out
    protected $settings = [
        'alter_owner' => false
    ];

    protected $schema;
    protected $diff;
    protected $summary;
    protected $master;
    protected $slave;

    public function __construct($schema)
    {
        $this->schema = $schema;
    }

    public function getDiff($master, $slave)
    {
        $this->master  = $master;
        $this->slave   = $slave;
        $this->diff    = [];
        $this->summary = [];

        $this->diffFunctions();
        $this->diffSequences();
        $this->diffViews();
        $this->diffTables();

        return [
            'diff'    => $this->diff,
            'summary' => $this->summary
        ];
    }
}