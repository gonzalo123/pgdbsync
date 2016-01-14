<?php

namespace Pgdbsync\Builder;

use Pgdbsync\DbConn;

class Conf
{
    private $conf;
    private $db;
    private $schemaDb;

    public function __construct(DbConn $db)
    {
        $this->conf = [];
        $this->db   = $db;
    }

    public function build($schema)
    {
        $this->conf     = [];
        $this->schemaDb = $this->db->schema($schema);
        $this->buildFunctions();
        $this->buildSecuences();
        $this->buildTables();
        $this->buildViews();

        return $this->conf;
    }

    private function buildFunctions()
    {
        /** @var \Pgdbsync\Functiondb $function */
        foreach ((array)$this->schemaDb->getFunctions() as $function) {
            $this->conf['functions'][$function->getName()]['definition'] = $function->getDefinition();
        }
    }

    private function buildSecuences()
    {
        /** @var \Pgdbsync\Sequence $sequence */
        foreach ((array)$this->schemaDb->getSequences() as $sequence) {
            $this->conf['sequences'][$sequence->getName()] = [
                'owner'      => $sequence->getOwner(),
                'increment'  => $sequence->getIncrement(),
                'minvalue'   => $sequence->getMinValue(),
                'maxvalue'   => $sequence->getMaxValue(),
                'startvalue' => $sequence->getStartValue()
            ];

            // Grants
            foreach ((array)$sequence->grants() as $grant) {
                $this->conf['sequences'][$sequence->getName()]['grants'][$grant] = $grant;
            }
        }
    }

    private function buildTables()
    {
        /** @var \Pgdbsync\Table $table */
        foreach ((array)$this->schemaDb->getTables() as $table) {
            $this->conf['tables'][$table->getName()] = [
                'owner'      => $table->getOwner(),
                'tablespace' => $table->getTablespace(),
                'oid'        => $table->getOid()
            ];

            $this->buildTableColumns($table);
            $this->buildTableConstraints($table);
        }
    }

    private function buildViews()
    {
        /** @var \Pgdbsync\View $view */
        foreach ((array)$this->schemaDb->getViews() as $view) {
            $this->conf['views'][$view->getName()] = [
                'owner'      => $view->getOwner(),
                'definition' => $view->getDefinition()
            ];

            // Grants
            foreach ((array)$view->grants() as $grant) {
                $this->conf['views'][$view->getName()]['grants'][$grant] = $grant;
            }
        }
    }

    private function buildTableColumns($table)
    {
        /** @var \Pgdbsync\Column $column */
        foreach ((array)$table->columns() as $column) {
            $this->conf['tables'][$table->getName()]['columns'][$column->getName()] = [
                'type'      => $column->getType(),
                'precision' => $column->getPrecision(),
                'nullable'  => $column->getIsNullable(),
                'order'     => $column->getOrder(),
            ];
        }
    }

    private function buildTableConstraints($table)
    {
        /** @var \Pgdbsync\Constraint $constraint */
        foreach ((array)$table->constraints() as $constraint) {
            $this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()] = [
                'type'          => $constraint->getType(),
                'src'           => $constraint->getConstraint(),
                'columns'       => $constraint->getColumns(),
                'reftable'      => $constraint->getReftable(),
                'delete_option' => $constraint->getOnDeleteOption(),
                'update_option' => $constraint->getOnUpdateOption(),
                'match_option'  => $constraint->getMatchOption(),
            ];
            if (!isset($this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()]['refcolumns'])) {
                $this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()]['refcolumns'] = [];
            }
            if (!in_array($constraint->getRefcolumn(), $this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()]['refcolumns'])) {
                $this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()]['refcolumns'][] = $constraint->getRefcolumn();
            }

        }

        // Grants
        foreach ((array)$table->grants() as $grant) {
            $this->conf['tables'][$table->getName()]['grants'][$grant] = $grant;
        }
    }
}