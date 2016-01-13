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
        $this->db = $db;
    }

    public function build($schema)
    {
        $this->conf      = [];
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
            $this->conf['sequences'][$sequence->getName()]['owner']      = $sequence->getOwner();
            $this->conf['sequences'][$sequence->getName()]['increment']  = $sequence->getIncrement();
            $this->conf['sequences'][$sequence->getName()]['minvalue']   = $sequence->getMinValue();
            $this->conf['sequences'][$sequence->getName()]['maxvalue']   = $sequence->getMaxValue();
            $this->conf['sequences'][$sequence->getName()]['startvalue'] = $sequence->getStartValue();

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

            $this->conf['tables'][$table->getName()]['owner']      = $table->getOwner();
            $this->conf['tables'][$table->getName()]['tablespace'] = $table->getTablespace();
            $this->conf['tables'][$table->getName()]['oid']        = $table->getOid();
            $this->buildTableColumns($table);
            $this->buildTableConstraints($table);
        }
    }

    private function buildViews()
    {
        /** @var \Pgdbsync\View $view */
        foreach ((array)$this->schemaDb->getViews() as $view) {
            $this->conf['views'][$view->getName()]['owner']      = $view->getOwner();
            $this->conf['views'][$view->getName()]['definition'] = $view->getDefinition();

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
            $this->conf['tables'][$table->getName()]['columns'][$column->getName()]['type']      = $column->getType();
            $this->conf['tables'][$table->getName()]['columns'][$column->getName()]['precision'] = $column->getPrecision();
            $this->conf['tables'][$table->getName()]['columns'][$column->getName()]['nullable']  = $column->getIsNullable();
            $this->conf['tables'][$table->getName()]['columns'][$column->getName()]['order']     = $column->getOrder();
        }
    }

    private function buildTableConstraints($table)
    {
        /** @var \Pgdbsync\Constraint $constraint */
        foreach ((array)$table->constraints() as $constraint) {
            $this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()]['type']          = $constraint->getType();
            $this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()]['src']           = $constraint->getConstraint();
            $this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()]['columns']       = $constraint->getColumns();
            $this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()]['reftable']      = $constraint->getReftable();
            $this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()]['delete_option'] = $constraint->getOnDeleteOption();
            $this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()]['update_option'] = $constraint->getOnUpdateOption();
            $this->conf['tables'][$table->getName()]['constraints'][$constraint->getName()]['match_option']  = $constraint->getMatchOption();
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