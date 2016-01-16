<?php

namespace Pgdbsync\Builder;

class Diff
{
    // @todo extract this configuration out
    private $settings = [
        'alter_owner' => false
    ];

    private $schema;
    private $diff;
    private $summary;

    public function __construct($schema)
    {
        $this->schema = $schema;
    }

    public function getDiff($master, $slave)
    {
        $this->diff    = [];
        $this->summary = [];

        // FUNCTIONS
        $masterFunctions = isset($master['functions']) ? array_keys((array)$master['functions']) : [];
        $slaveFunctions  = isset($slave['functions']) ? array_keys((array)$slave['functions']) : [];

        // delete deleted functions
        $deletedFunctions = array_diff($slaveFunctions, $masterFunctions);
        if (count($deletedFunctions) > 0) {
            $this->deleteFunctions($deletedFunctions, $master);
        }
        // create new functions
        $newFunctions = array_diff($masterFunctions, $slaveFunctions);

        // check differences
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
            $this->createFunctions($newFunctions, $master);
        }

        // SEQUENCES
        $masterSequences = isset($master['sequences']) ? array_keys((array)$master['sequences']) : [];
        $slaveSequences  = isset($slave['sequences']) ? array_keys((array)$slave['sequences']) : [];

        // delete deleted sequences
        $deletedSequences = array_diff($slaveSequences, $masterSequences);
        if (count($deletedSequences) > 0) {
            $this->deleteSequences($deletedSequences, $master);
        }
        // create new sequences
        $newSequences = array_diff($masterSequences, $slaveSequences);
        if (count($newSequences) > 0) {
            $this->createSequences($newSequences, $master);
        }

        // VIEWS
        $masterViews = isset($master['views']) ? array_keys((array)$master['views']) : [];
        $slaveViews  = isset($slave['views']) ? array_keys((array)$slave['views']) : [];

        // delete deleted views
        $deletedViews = array_diff($slaveViews, $masterViews);
        if (count($deletedViews) > 0) {
            $this->deleteViews($deletedViews, $master);
        }

        // create new views
        $newViews = array_diff($masterViews, $slaveViews);
        if (count($newViews) > 0) {
            $this->createViews($newViews, $master);
        }

        foreach ($masterViews as $view) {
            if (in_array($view, $newViews)) {
                continue;
            }

            if ($master['views'][$view]['definition'] !== $slave['views'][$view]['definition']) {
                $this->createView($view, $master);
            }
        }
        // TABLES

        $masterTables = isset($master['tables']) ? array_keys((array)$master['tables']) : [];
        $slaveTables  = isset($slave['tables']) ? array_keys((array)$slave['tables']) : [];

        // delete deleted tables
        $deletedTables = array_diff($slaveTables, $masterTables);
        if (count($deletedTables) > 0) {
            $this->deleteTables($deletedTables, $master);
        }

        // create new tables
        $newTables = array_diff($masterTables, $slaveTables);
        if (count($newTables) > 0) {
            $this->createTables($newTables, $master);
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
                $this->addColumns($table, $newColumns, $master);
            }

            $deletedColumns = array_diff($slaveColumns, $masterColumns);
            $this->deleteColumns($table, $deletedColumns, $master);

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
                        $this->alterColumn($table, $column, $master);
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
                    $this->dropConstraint($table, $deletedConstraint);
                }
            }
            // then add the new constraints
            $newConstraints = array_diff($masterConstraints, $slaveConstraints);
            if (count($newConstraints) > 0) {
                foreach ($newConstraints as $newConstraint) {
                    $this->addConstraint($table, $newConstraint, $master);
                }
            }
        }

        return [
            'diff'    => $this->diff,
            'summary' => $this->summary
        ];
    }

    private function createTables($tables, $master)
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
                $buffer  = "\nCREATE TABLE {$this->schema}.{$table}(\n {$columns}\n)";
                if (!empty($tablespace)) {
                    $buffer .= "\nTABLESPACE {$tablespace}";
                }
                $buffer .= ";";

                if ($this->settings['alter_owner'] === true) {
                    $buffer .= "\nALTER TABLE {$this->schema}.{$table} OWNER TO {$owner};";
                }

                if (array_key_exists('grants', $master['tables'][$table])) {
                    foreach ((array)$master['tables'][$table]['grants'] as $grant) {
                        if (!empty($grant)) {
                            $buffer .= "\nGRANT ALL ON TABLE {$this->schema}.{$table} TO {$grant};";
                        }
                    }
                }
                $this->diff[]                        = $buffer;
                $this->summary['tables']['create'][] = "{$this->schema}.{$table}";
            }
        }
    }

    private function deleteTables($tables, $master)
    {
        if (count((array)$tables) > 0) {
            foreach ($tables as $table) {
                $this->diff[]                      = "\nDROP TABLE {$this->schema}.{$table};";
                $this->summary['tables']['drop'][] = "{$this->schema}.{$table}";
            }
        }
    }

    private function deleteViews($views, $master)
    {
        if (count((array)$views) > 0) {
            foreach ($views as $view) {
                $this->diff[]                     = "DROP VIEW {$this->schema}.{$view};";
                $this->summary['views']['drop'][] = "{$this->schema}.{$view}";
            }
        }
    }

    private function deleteSequences($sequences, $master)
    {
        if (count((array)$sequences) > 0) {
            foreach ($sequences as $sequence) {
                $this->diff[]                        = "DROP SEQUENCE {$this->schema}.{$sequence};";
                $this->summary['sequence']['drop'][] = "{$this->schema}.{$sequence}";
            }
        }
    }

    private function deleteFunctions($functions, $master)
    {
        if (count((array)$functions) > 0) {
            foreach ($functions as $function) {
                $this->diff[]                        = "DROP FUNCTION {$function};";
                $this->summary['function']['drop'][] = "{$function}";
            }
        }
    }

    private function createFunctions($functions, $master)
    {
        if (count((array)$functions) > 0) {
            foreach ($functions as $function) {
                $buffer                          = $master['functions'][$function]['definition'];
                $this->summary['function']['create'][] = "{$this->schema}.{$function}";
                $this->diff[]                          = $buffer;
            }
        }
    }

    private function createSequences($sequences, $master)
    {
        if (count((array)$sequences) > 0) {
            foreach ($sequences as $sequence) {
                $this->createSequence($sequence, $master);
            }
        }
    }

    private function createSequence($sequence, $master)
    {
        $increment = $master['sequences'][$sequence]['increment'];
        $minvalue  = $master['sequences'][$sequence]['minvalue'];
        $maxvalue  = $master['sequences'][$sequence]['maxvalue'];
        $start     = $master['sequences'][$sequence]['startvalue'];

        $owner  = $master['sequences'][$sequence]['owner'];
        $buffer = "\nCREATE SEQUENCE {$this->schema}.{$sequence}";
        $buffer .= "\n  INCREMENT {$increment}";
        $buffer .= "\n  MINVALUE {$minvalue}";
        $buffer .= "\n  MAXVALUE {$maxvalue}";
        $buffer .= "\n  START 1;";
        if ($this->settings['alter_owner'] === true) {
            $buffer .= "\nALTER TABLE {$this->schema}.{$sequence} OWNER TO {$owner};";
        }
        foreach ($master['sequences'][$sequence]['grants'] as $grant) {
            if (!empty($grant)) {
                $buffer .= "\nGRANT ALL ON TABLE {$this->schema}.{$sequence} TO {$grant};";
            }
        }
        $this->diff[]                          = $buffer;
        $this->summary['secuence']['create'][] = "{$this->schema}.{$sequence}";
    }

    private function createView($view, $master)
    {
        $definition = $master['views'][$view]['definition'];
        $owner      = $master['views'][$view]['owner'];
        $buffer     = "\nCREATE OR REPLACE VIEW {$this->schema}.{$view} AS\n";
        $buffer .= "  " . $definition;
        if ($this->settings['alter_owner'] === true) {
            $buffer .= "\nALTER TABLE {$this->schema}.{$view} OWNER TO {$owner};";
        }
        foreach ($master['views'][$view]['grants'] as $grant) {
            if (!empty($grant)) {
                $buffer .= "\nGRANT ALL ON TABLE {$this->schema}.{$view} TO {$grant};";
            }
        }
        $this->diff[]                      = $buffer;
        $this->summary['view']['create'][] = "{$this->schema}.{$view}";
    }

    private function createViews($views, $master)
    {
        if (count((array)$views) > 0) {
            foreach ($views as $view) {
                $this->createView($view, $master);
            }
        }
    }

    private function addColumns($table, $columns, $master)
    {
        if (count((array)$columns) > 0) {
            foreach ($columns as $column) {
                $this->diff[]                        = "ADD COLUMN {$column} TO TABLE {$table}";
                $this->summary['column']['create'][] = "{$this->schema}.{$table}.{$column}";
            }
        }
    }

    private function deleteColumns($table, $columns, $master)
    {
        if (count((array)$columns) > 0) {
            foreach ($columns as $column) {
                $this->diff[]                      = "DELETE COLUMN {$column} TO TABLE {$table}";
                $this->summary['column']['drop'][] = "{$this->schema}.{$table} {$column}";
            }
        }
    }

    private function alterColumn($table, $column, $master)
    {
        $masterType                   = $master['tables'][$table]['columns'][$column]['type'];
        $masterPrecision              = $master['tables'][$table]['columns'][$column]['precision'];
        $this->diff[]                       = "ALTER TABLE {$this->schema}.{$table} ALTER {$column} TYPE {$masterType}" . (empty($masterPrecision) ? "" : ("(" . $masterPrecision . ")")) . ";";
        $this->summary['column']['alter'][] = "{$this->schema}.{$table} {$column}";
    }

    private function addConstraint($table, $constraint, $master)
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
                    $this->diff[] = "ALTER TABLE {$this->schema}.{$table} ADD CONSTRAINT {$constraint} {$type} (" . implode(', ', $columns) . ");";
                } else {
                    $this->summary[] = 'CONSTRAINT ' . $constraint . ' FOR TABLE ' . $this->schema . '.' . $table . ' COULD NOT BE ADDED BECAUSE NO COLUMNS WERE DETECTED';
                }
                break;
            case 'CHECK':
                if (!empty($columns)) {
                    $this->diff[] = "ALTER TABLE {$this->schema}.{$table} ADD CONSTRAINT {$constraint} {$type} CHECK {$constraintSrc}";
                } else {
                    $this->summary[] = 'CONSTRAINT ' . $constraint . ' FOR TABLE ' . $this->schema . '.' . $table . ' COULD NOT BE ADDED BECAUSE NO COLUMNS WERE DETECTED';
                }
                break;
            case 'PRIMARY KEY':
                if (!empty($columns)) {
                    $this->diff[] = "ALTER TABLE {$this->schema}.{$table} ADD CONSTRAINT {$constraint} {$type} (" . implode(', ', $columns) . ");";
                } else {
                    $this->summary[] = 'CONSTRAINT ' . $constraint . ' FOR TABLE ' . $this->schema . '.' . $table . ' COULD NOT BE ADDED BECAUSE NO COLUMNS WERE DETECTED';
                }
                break;
            case 'FOREIGN KEY':
                $fkSchema  = $this->schema;
                $fkTable   = $constraintData['reftable'];
                $fkColumns = $constraintData['refcolumns'];
                if (!empty($columns) && !empty($fkTable) && !empty($fkColumns)) {
                    $deleteAction = strtoupper(Constraint::$ON_ACTION_MAP[$constraintData['delete_option']]);
                    $updateAction = strtoupper(Constraint::$ON_ACTION_MAP[$constraintData['update_option']]);
                    $match        = strtoupper(Constraint::$MATCH_MAP[$constraintData['match_option']]);
                    $this->diff[]       = "ALTER TABLE {$this->schema}.{$table}
                    ADD CONSTRAINT {$constraint} {$type} (" . implode(', ', $columns) . ")
                    REFERENCES {$fkSchema}.{$fkTable} (" . implode(', ', $fkColumns) . ")  MATCH {$match}
                    ON UPDATE {$updateAction} ON DELETE {$deleteAction};";
                } else {
                    $this->summary[] = 'CONSTRAINT ' . $constraint . ' FOR TABLE ' . $this->schema . '.' . $table . ' COULD NOT BE ADDED BECAUSE NO COLUMNS WERE DETECTED';
                }
                break;
        }
    }

    private function dropConstraint($table, $constraint)
    {
        $this->diff[] = "ALTER TABLE {$this->schema}.{$table} DROP CONSTRAINT {$constraint} CASCADE;";
    }

}