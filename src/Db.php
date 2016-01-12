<?php
namespace Pgdbsync;

class Db
{

    private $settings = [
        'alter_owner' => false
    ];

    private $masterDb = null;

    public function setMasrer(DbConn $db)
    {
        $this->masterDb = $db;
    }

    private $slaveDb = [];

    public function setSlave(DbConn $db)
    {
        $this->slaveDb[] = $db;
    }

    private function _buildConf(DbConn $db, $schema)
    {
        $out      = [];
        $schemaDb = $db->schema($schema);

        // functions
        /** @var Functiondb $function */
        foreach ((array)$schemaDb->getFunctions() as $function) {
            $out['functions'][$function->getName()]['definition'] = $function->getDefinition();
        }

        // Sequences
        /** @var Sequence $sequence */
        foreach ((array)$schemaDb->getSequences() as $sequence) {
            $out['sequences'][$sequence->getName()]['owner']      = $sequence->getOwner();
            $out['sequences'][$sequence->getName()]['increment']  = $sequence->getIncrement();
            $out['sequences'][$sequence->getName()]['minvalue']   = $sequence->getMinValue();
            $out['sequences'][$sequence->getName()]['maxvalue']   = $sequence->getMaxValue();
            $out['sequences'][$sequence->getName()]['startvalue'] = $sequence->getStartValue();

            // Grants
            foreach ((array)$sequence->grants() as $grant) {
                $out['sequences'][$sequence->getName()]['grants'][$grant] = $grant;
            }
        }

        // tables
        /** @var Table $table */
        foreach ((array)$schemaDb->getTables() as $table) {

            $out['tables'][$table->getName()]['owner']      = $table->getOwner();
            $out['tables'][$table->getName()]['tablespace'] = $table->getTablespace();
            $out['tables'][$table->getName()]['oid']        = $table->getOid();
            // Columns
            /** @var Column $column */
            foreach ((array)$table->columns() as $column) {
                $out['tables'][$table->getName()]['columns'][$column->getName()]['type']      = $column->getType();
                $out['tables'][$table->getName()]['columns'][$column->getName()]['precision'] = $column->getPrecision();
                $out['tables'][$table->getName()]['columns'][$column->getName()]['nullable']  = $column->getIsNullable();
                $out['tables'][$table->getName()]['columns'][$column->getName()]['order']     = $column->getOrder();
            }
            // Constraints
            /** @var Constraint $constraint */
            foreach ((array)$table->constraints() as $constraint) {
                $out['tables'][$table->getName()]['constraints'][$constraint->getName()]['type']          = $constraint->getType();
                $out['tables'][$table->getName()]['constraints'][$constraint->getName()]['src']           = $constraint->getConstraint();
                $out['tables'][$table->getName()]['constraints'][$constraint->getName()]['columns']       = $constraint->getColumns();
                $out['tables'][$table->getName()]['constraints'][$constraint->getName()]['reftable']      = $constraint->getReftable();
                $out['tables'][$table->getName()]['constraints'][$constraint->getName()]['delete_option'] = $constraint->getOnDeleteOption();
                $out['tables'][$table->getName()]['constraints'][$constraint->getName()]['update_option'] = $constraint->getOnUpdateOption();
                $out['tables'][$table->getName()]['constraints'][$constraint->getName()]['match_option']  = $constraint->getMatchOption();
                if (!isset($out['tables'][$table->getName()]['constraints'][$constraint->getName()]['refcolumns'])) {
                    $out['tables'][$table->getName()]['constraints'][$constraint->getName()]['refcolumns'] = [];
                }
                if (!in_array($constraint->getRefcolumn(), $out['tables'][$table->getName()]['constraints'][$constraint->getName()]['refcolumns'])) {
                    $out['tables'][$table->getName()]['constraints'][$constraint->getName()]['refcolumns'][] = $constraint->getRefcolumn();
                }

            }
            // Grants
            foreach ((array)$table->grants() as $grant) {
                $out['tables'][$table->getName()]['grants'][$grant] = $grant;
            }
        }

        // Views
        /** @var View $view */
        foreach ((array)$schemaDb->getViews() as $view) {
            $out['views'][$view->getName()]['owner']      = $view->getOwner();
            $out['views'][$view->getName()]['definition'] = $view->getDefinition();

            // Grants
            foreach ((array)$view->grants() as $grant) {
                $out['views'][$view->getName()]['grants'][$grant] = $grant;
            }
        }

        return $out;
    }

    private function _createTables($schema, $tables, $master, &$diff, &$summary)
    {
        if (count((array)$tables) > 0) {
            foreach ($tables as $table) {
                $tablespace = $master['tables'][$table]['tablespace'];
                $_columns   = [];
                foreach ((array)$master['tables'][$table]['columns'] as $column => $columnConf) {
                    $type       = $columnConf['type'];
                    $precision  = $columnConf['precision'];
                    $nullable   = $columnConf['nullable'] ? null : ' NOT NULL';
                    $_columns[] = "{$column} {$type}" . ((!empty($precision)) ? "({$precision})" : null) . $nullable;
                }
                if (array_key_exists('constraints', $master['tables'][$table])) {
                    foreach ((array)$master['tables'][$table]['constraints'] as $constraint => $constraintInfo) {
                        $columns       = [];
                        $masterColumns = $master['tables'][$table]['columns'];
                        foreach ($constraintInfo['columns'] as $column) {
                            foreach ($masterColumns as $masterColumnName => $masterColumn) {
                                if ($masterColumn['order'] == $column) {
                                    $columns[] = $masterColumnName;
                                }
                            }
                        }
                        switch ($constraintInfo['type']) {
                            case 'CHECK':
                                $constraintSrc = $constraintInfo['src'];
                                $_columns[]    = "CONSTRAINT {$constraint} CHECK {$constraintSrc}";
                                break;
                            case 'PRIMARY KEY':
                                $constraintSrc = $constraintInfo['src'];
                                $columns       = implode(', ', $columns);
                                $_columns[]    = "CONSTRAINT {$constraint} PRIMARY KEY ({$columns})";
                                break;
                        }
                    }
                }
                $owner   = $master['tables'][$table]['owner'];
                $columns = implode(",\n ", $_columns);
                $buffer  = "\nCREATE TABLE {$schema}.{$table}(\n {$columns}\n)";
                if (!empty($tablespace)) {
                    $buffer .= "\nTABLESPACE {$tablespace}";
                }
                $buffer .= ";";

                if ($this->settings['alter_owner'] === true) {
                    $buffer .= "\nALTER TABLE {$schema}.{$table} OWNER TO {$owner};";
                }

                if (array_key_exists('grants', $master['tables'][$table])) {
                    foreach ((array)$master['tables'][$table]['grants'] as $grant) {
                        if (!empty($grant)) {
                            $buffer .= "\nGRANT ALL ON TABLE {$schema}.{$table} TO {$grant};";
                        }
                    }
                }
                $diff[]                        = $buffer;
                $summary['tables']['create'][] = "{$schema}.{$table}";
            }
        }
    }

    private function _deleteTables($schema, $tables, $master, &$diff, &$summary)
    {
        if (count((array)$tables) > 0) {
            foreach ($tables as $table) {
                $diff[]                      = "\nDROP TABLE {$schema}.{$table};";
                $summary['tables']['drop'][] = "{$schema}.{$table}";
            }
        }
    }

    private function _deleteViews($schema, $views, $master, &$diff, &$summary)
    {
        if (count((array)$views) > 0) {
            foreach ($views as $view) {
                $diff[]                     = "DROP VIEW {$schema}.{$view};";
                $summary['views']['drop'][] = "{$schema}.{$view}";
            }
        }
    }

    private function _deleteSequences($schema, $sequences, $master, &$diff, &$summary)
    {
        if (count((array)$sequences) > 0) {
            foreach ($sequences as $sequence) {
                $diff[]                        = "DROP SEQUENCE {$schema}.{$sequence};";
                $summary['sequence']['drop'][] = "{$schema}.{$sequence}";
            }
        }
    }

    private function _deleteFunctions($schema, $functions, $master, &$diff, &$summary)
    {
        if (count((array)$functions) > 0) {
            foreach ($functions as $function) {
                $diff[]                        = "DROP FUNCTION {$function};";
                $summary['function']['drop'][] = "{$function}";
            }
        }
    }

    private function _createFunctions($schema, $functions, $master, &$diff, &$summary)
    {
        if (count((array)$functions) > 0) {
            foreach ($functions as $function) {
                $buffer                          = $master['functions'][$function]['definition'];
                $summary['function']['create'][] = "{$schema}.{$function}";
                $diff[]                          = $buffer;
            }
        }
    }

    private function _createSequences($schema, $sequences, $master, &$diff, &$summary)
    {
        if (count((array)$sequences) > 0) {
            foreach ($sequences as $sequence) {
                $this->_createSequence($schema, $sequence, $master, $diff, $summary);
            }
        }
    }

    private function _createSequence($schema, $sequence, $master, &$diff, &$summary)
    {
        $increment = $master['sequences'][$sequence]['increment'];
        $minvalue  = $master['sequences'][$sequence]['minvalue'];
        $maxvalue  = $master['sequences'][$sequence]['maxvalue'];
        $start     = $master['sequences'][$sequence]['startvalue'];

        $owner  = $master['sequences'][$sequence]['owner'];
        $buffer = "\nCREATE SEQUENCE {$schema}.{$sequence}";
        $buffer .= "\n  INCREMENT {$increment}";
        $buffer .= "\n  MINVALUE {$minvalue}";
        $buffer .= "\n  MAXVALUE {$maxvalue}";
        $buffer .= "\n  START 1;";
        if ($this->settings['alter_owner'] === true) {
            $buffer .= "\nALTER TABLE {$schema}.{$sequence} OWNER TO {$owner};";
        }
        foreach ($master['sequences'][$sequence]['grants'] as $grant) {
            if (!empty($grant)) {
                $buffer .= "\nGRANT ALL ON TABLE {$schema}.{$sequence} TO {$grant};";
            }
        }
        $diff[]                          = $buffer;
        $summary['secuence']['create'][] = "{$schema}.{$sequence}";
    }

    private function _createView($schema, $view, $master, &$diff, &$summary)
    {
        $definition = $master['views'][$view]['definition'];
        $owner      = $master['views'][$view]['owner'];
        $buffer     = "\nCREATE OR REPLACE VIEW {$schema}.{$view} AS\n";
        $buffer .= "  " . $definition;
        if ($this->settings['alter_owner'] === true) {
            $buffer .= "\nALTER TABLE {$schema}.{$view} OWNER TO {$owner};";
        }
        foreach ($master['views'][$view]['grants'] as $grant) {
            if (!empty($grant)) {
                $buffer .= "\nGRANT ALL ON TABLE {$schema}.{$view} TO {$grant};";
            }
        }
        $diff[]                      = $buffer;
        $summary['view']['create'][] = "{$schema}.{$view}";
    }

    private function _createViews($schema, $views, $master, &$diff, &$summary)
    {
        if (count((array)$views) > 0) {
            foreach ($views as $view) {
                $this->_createView($schema, $view, $master, $diff, $summary);
            }
        }
    }

    private function _addColumns($schema, $table, $columns, $master, &$diff, &$summary)
    {
        if (count((array)$columns) > 0) {
            foreach ($columns as $column) {
                $diff[]                        = "ADD COLUMN {$column} TO TABLE {$table}";
                $summary['column']['create'][] = "{$schema}.{$table}.{$column}";
            }
        }
    }

    private function _deleteColumns($schema, $table, $columns, $master, &$diff, &$summary)
    {
        if (count((array)$columns) > 0) {
            foreach ($columns as $column) {
                $diff[]                      = "DELETE COLUMN {$column} TO TABLE {$table}";
                $summary['column']['drop'][] = "{$schema}.{$table} {$column}";
            }
        }
    }

    private function _alterColumn($schema, $table, $column, $master, &$diff, &$summary)
    {
        $masterType                   = $master['tables'][$table]['columns'][$column]['type'];
        $masterPrecision              = $master['tables'][$table]['columns'][$column]['precision'];
        $diff[]                       = "ALTER TABLE {$schema}.{$table} ALTER {$column} TYPE {$masterType}" . (empty($masterPrecision) ? "" : ("(" . $masterPrecision . ")")) . ";";
        $summary['column']['alter'][] = "{$schema}.{$table} {$column}";
    }

    public function summary($schema)
    {
        $buffer = [];
        $data   = $this->_diff($schema);
        foreach ($data as $row) {
            if (count($row['summary']) > 0) {
                $title    = "DBNAME : " . $row['db']->dbName();
                $buffer[] = $title;
                $buffer[] = str_repeat("-", strlen($title));

                foreach ($row['summary'] as $type => $info) {
                    $buffer[] = $type;
                    foreach ($info as $mode => $objects) {
                        foreach ($objects as $object) {
                            $buffer[] = " " . $mode . " :: " . $object;
                        }
                    }
                }
                $buffer[] = "\n";
            }
        }

        return implode("\n", $buffer) . "\n";
    }

    public function run($schema)
    {
        $errors = [];
        $data   = $this->_diff($schema);
        foreach ($data as $row) {
            /** @var DbConn $db */
            $db   = $row['db'];
            $host = $db->dbHost() . " :: " . $db->dbName();
            foreach ($row['diff'] as $item) {
                try {
                    $db->exec($item);
                } catch (\PDOException $e) {
                    $errors[$host][] = [
                        $item,
                        $e->getMessage()
                    ];
                }
            }
        }

        return $errors;
    }

    public function raw($schema)
    {
        return $this->_diff($schema);
    }

    public function diff($schema)
    {
        $buffer = [];
        $data   = $this->_diff($schema);
        foreach ($data as $row) {
            if (count($row['diff']) > 0) {
                $title    = "DBNAME : " . $row['db']->dbName();
                $buffer[] = $title;
                $buffer[] = str_repeat("-", strlen($title));

                foreach ($row['diff'] as $item) {
                    $buffer[] = $item;
                }
                $buffer[] = "\n";
            } else {
                $buffer[] = "Already sync : " . $row['db']->dbName();
            }
        }

        return implode("\n", $buffer) . "\n";
    }

    private function _diff($schema)
    {
        $out    = [];
        $master = $this->_buildConf($this->masterDb->connect(), $schema);
        foreach ($this->slaveDb as $slaveDb) {
            $slave = $this->_buildConf($slaveDb->connect(), $schema);
            if (md5(serialize($master)) == md5(serialize($slave))) {
                // echo "[OK] <b>{$schema}</b> " . $slaveDb->dbName() . "<br/>";
                $out[] = [
                    'db'      => $slaveDb,
                    'diff'    => [],
                    'summary' => []
                ];
            } else {
                $diff = $summary = [];

                // FUNCTIONS
                $masterFunctions = isset($master['functions']) ? array_keys((array)$master['functions']) : [];
                $slaveFunctions  = isset($slave['functions']) ? array_keys((array)$slave['functions']) : [];
                // delete deleted functions
                $deletedFunctions = array_diff($slaveFunctions, $masterFunctions);
                if (count($deletedFunctions) > 0) {
                    $this->_deleteFunctions($schema, $deletedFunctions, $master, $diff, $summary);
                }
                // create new functions
                $newFunctions = array_diff($masterFunctions, $slaveFunctions);

                // check diferences
                foreach ($masterFunctions as $functionName) {
                    if (!in_array($functionName, $newFunctions)) {
                        $definitionMaster = $master['functions'][$functionName]['definition'];
                        $definitionSlave  = $slave['functions'][$functionName]['definition'];

                        if (md5($definitionMaster) != md5($definitionSlave)) {
                            $newFunctions[] = $functionName;
                        }
                    }
                }

                if (count($newFunctions) > 0) {
                    $this->_createFunctions($schema, $newFunctions, $master, $diff, $summary);
                }

                // SEQUENCES
                $masterSequences = isset($master['sequences']) ? array_keys((array)$master['sequences']) : [];
                $slaveSequences  = isset($slave['sequences']) ? array_keys((array)$slave['sequences']) : [];

                // delete deleted sequences
                $deletedSequences = array_diff($slaveSequences, $masterSequences);
                if (count($deletedSequences) > 0) {
                    $this->_deleteSequences($schema, $deletedSequences, $master, $diff, $summary);
                }
                // create new sequences
                $newSequences = array_diff($masterSequences, $slaveSequences);
                if (count($newSequences) > 0) {
                    $this->_createSequences($schema, $newSequences, $master, $diff, $summary);
                }

                // VIEWS
                $masterViews = isset($master['views']) ? array_keys((array)$master['views']) : [];
                $slaveViews  = isset($slave['views']) ? array_keys((array)$slave['views']) : [];

                // delete deleted views
                $deletedViews = array_diff($slaveViews, $masterViews);
                if (count($deletedViews) > 0) {
                    $this->_deleteViews($schema, $deletedViews, $master, $diff, $summary);
                }

                // create new views
                $newViews = array_diff($masterViews, $slaveViews);
                if (count($newViews) > 0) {
                    $this->_createViews($schema, $newViews, $master, $diff, $summary);
                }

                foreach ($masterViews as $view) {
                    if (in_array($view, $newViews)) {
                        continue;
                    }

                    if ($master['views'][$view]['definition'] !== $slave['views'][$view]['definition']) {
                        $this->_createView($schema, $view, $master, $diff, $summary);
                    }
                }
                // TABLES

                $masterTables = isset($master['tables']) ? array_keys((array)$master['tables']) : [];
                $slaveTables  = isset($slave['tables']) ? array_keys((array)$slave['tables']) : [];

                // delete deleted tables
                $deletedTables = array_diff($slaveTables, $masterTables);
                if (count($deletedTables) > 0) {
                    $this->_deleteTables($schema, $deletedTables, $master, $diff, $summary);
                }

                // create new tables
                $newTables = array_diff($masterTables, $slaveTables);
                if (count($newTables) > 0) {
                    $this->_createTables($schema, $newTables, $master, $diff, $summary);
                }

                foreach ($masterTables as $table) {
                    if (in_array($table, $newTables)) {
                        continue;
                    }

                    // check new columns in $master and not in $slave
                    // check deleted columns in $master (exits in $slave and not in master)
                    $masterColumns = array_keys((array)$master['tables'][$table]['columns']);
                    $slaveColumns  = array_keys((array)$slave['tables'][$table]['columns']);

                    $newColumns = array_diff($masterColumns, $slaveColumns);
                    if (count($newColumns) > 0) {
                        $this->_addColumns($schema, $table, $newColumns, $master, $diff, $summary);
                    }

                    $deletedColumns = array_diff($slaveColumns, $masterColumns);
                    $this->_deleteColumns($schema, $table, $deletedColumns, $master, $diff, $summary);

                    foreach ($masterColumns as $column) {
                        // check modifications (different between $master and $slave)
                        // check differences in type
                        if (isset($master['tables'][$table]['columns'][$column]) && isset($slave['tables'][$table]['columns'][$column])) {
                            $masterType = $master['tables'][$table]['columns'][$column]['type'];
                            $slaveType  = $slave['tables'][$table]['columns'][$column]['type'];
                            // check differences in precission
                            $masterPrecission = $master['tables'][$table]['columns'][$column]['precision'];
                            $slavePrecission  = $slave['tables'][$table]['columns'][$column]['precision'];

                            if ($masterType != $slaveType || $masterPrecission != $slavePrecission) {
                                $this->_alterColumn($schema, $table, $column, $master, $diff, $summary);
                            }
                        }
                    }

                    // check new or removed constraints
                    $masterConstraints = array_keys((array)$master['tables'][$table]['constraints']);
                    $slaveConstraints  = array_keys((array)$slave['tables'][$table]['constraints']);
                    // Delete missing constraints first
                    $deletedConstraints = array_diff($slaveConstraints, $masterConstraints);
                    if (count($deletedConstraints) > 0) {
                        foreach ($deletedConstraints as $deletedConstraint) {
                            $this->_dropConstraint($schema, $table, $deletedConstraint, $diff, $summary);
                        }
                    }
                    // then add the new constraints                    
                    $newConstraints = array_diff($masterConstraints, $slaveConstraints);
                    if (count($newConstraints) > 0) {
                        foreach ($newConstraints as $newConstraint) {
                            $this->_addConstraint($schema, $table, $newConstraint, $master, $diff, $summary);
                        }
                    }

                }
                $out[] = [
                    'db'      => $slaveDb,
                    'diff'    => $diff,
                    'summary' => $summary
                ];
            }
        }

        return $out;
    }

    private function _addConstraint($schema, $table, $constraint, $master, &$diff, &$summary)
    {
        $constraintData = $master['tables'][$table]['constraints'][$constraint];
        $type           = strtoupper($constraintData['type']);
        $columns        = [];
        $masterColumns  = $master['tables'][$table]['columns'];
        foreach ($constraintData['columns'] as $column) {
            foreach ($masterColumns as $masterColumnName => $masterColumn) {
                if ($masterColumn['order'] == $column) {
                    $columns[] = $masterColumnName;
                }
            }
        }

        switch ($type) {
            case 'UNIQUE':
                if (!empty($columns)) {
                    $diff[] = "ALTER TABLE {$schema}.{$table} ADD CONSTRAINT {$constraint} {$type} (" . implode(', ', $columns) . ");";
                } else {
                    $summary[] = 'CONSTRAINT ' . $constraint . ' FOR TABLE ' . $schema . '.' . $table . ' COULD NOT BE ADDED BECAUSE NO COLUMNS WERE DETECTED';
                }
                break;
            case 'CHECK':
                if (!empty($columns)) {
                    $diff[] = "ALTER TABLE {$schema}.{$table} ADD CONSTRAINT {$constraint} {$type} CHECK {$constraintSrc}";
                } else {
                    $summary[] = 'CONSTRAINT ' . $constraint . ' FOR TABLE ' . $schema . '.' . $table . ' COULD NOT BE ADDED BECAUSE NO COLUMNS WERE DETECTED';
                }
                break;
            case 'PRIMARY KEY':
                if (!empty($columns)) {
                    $diff[] = "ALTER TABLE {$schema}.{$table} ADD CONSTRAINT {$constraint} {$type} (" . implode(', ', $columns) . ");";
                } else {
                    $summary[] = 'CONSTRAINT ' . $constraint . ' FOR TABLE ' . $schema . '.' . $table . ' COULD NOT BE ADDED BECAUSE NO COLUMNS WERE DETECTED';
                }
                break;
            case 'FOREIGN KEY':
                $fkSchema  = $schema;
                $fkTable   = $constraintData['reftable'];
                $fkColumns = $constraintData['refcolumns'];
                if (!empty($columns) && !empty($fkTable) && !empty($fkColumns)) {
                    $deleteAction = strtoupper(Constraint::$ON_ACTION_MAP[$constraintData['delete_option']]);
                    $updateAction = strtoupper(Constraint::$ON_ACTION_MAP[$constraintData['update_option']]);
                    $match        = strtoupper(Constraint::$MATCH_MAP[$constraintData['match_option']]);
                    $diff[]       = "ALTER TABLE {$schema}.{$table}
                    ADD CONSTRAINT {$constraint} {$type} (" . implode(', ', $columns) . ")
                    REFERENCES {$fkSchema}.{$fkTable} (" . implode(', ', $fkColumns) . ")  MATCH {$match}
                    ON UPDATE {$updateAction} ON DELETE {$deleteAction};";
                } else {
                    $summary[] = 'CONSTRAINT ' . $constraint . ' FOR TABLE ' . $schema . '.' . $table . ' COULD NOT BE ADDED BECAUSE NO COLUMNS WERE DETECTED';
                }
                break;
        }
    }

    private function _dropConstraint($schema, $table, $constraint, &$diff, &$summary)
    {
        $diff[] = "ALTER TABLE {$schema}.{$table} DROP CONSTRAINT {$constraint} CASCADE;";
    }
}
