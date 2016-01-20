<?php

namespace Pgdbsync\Builder\Diff;

use Pgdbsync\Constraint;

trait TablesTrait
{
    protected function diffTables()
    {
        $masterTables = isset($this->master['tables']) ? array_keys((array)$this->master['tables']) : [];
        $slaveTables  = isset($this->slave['tables']) ? array_keys((array)$this->slave['tables']) : [];

        // delete deleted tables
        $deletedTables = array_diff($slaveTables, $masterTables);
        if (count($deletedTables) > 0) {
            $this->deleteTables($deletedTables);
        }

        // create new tables
        $newTables = array_diff($masterTables, $slaveTables);
        if (count($newTables) > 0) {
            $this->createTables($newTables);
        }

        foreach ($masterTables as $table) {
            if (in_array($table, $newTables)) {
                continue;
            }

            // check new columns in $master and not in slave
            // check deleted columns in $master (exits in slave and not in master)
            $masterColumns = array_keys((array)$this->master['tables'][$table]['columns']);
            $slaveColumns  = array_keys((array)$this->slave['tables'][$table]['columns']);

            $newColumns = array_diff($masterColumns, $slaveColumns);
            if (count($newColumns) > 0) {
                $this->addColumns($table, $newColumns);
            }

            $deletedColumns = array_diff($slaveColumns, $masterColumns);
            $this->deleteColumns($table, $deletedColumns);

            foreach ($masterColumns as $column) {
                // check modifications (different between $master and slave)
                // check differences in type
                if (isset($this->master['tables'][$table]['columns'][$column]) && isset($this->slave['tables'][$table]['columns'][$column])) {
                    $masterType = $this->master['tables'][$table]['columns'][$column]['type'];
                    $slaveType  = $this->slave['tables'][$table]['columns'][$column]['type'];
                    // check differences in precission
                    $masterPrecission = $this->master['tables'][$table]['columns'][$column]['precision'];
                    $slavePrecission  = $this->slave['tables'][$table]['columns'][$column]['precision'];

                    if ($masterType != $slaveType || $masterPrecission != $slavePrecission) {
                        $this->alterColumn($table, $column);
                    }
                }
            }

            // check new or removed constraints
            if (isset($this->master['tables'][$table]['constraints'])) {
                $masterConstraints = array_keys((array)$this->master['tables'][$table]['constraints']);
            } else {
                $masterConstraints = [];
            }

            if (isset($this->slave['tables'][$table]['constraints'])) {
                $slaveConstraints = array_keys((array)$this->slave['tables'][$table]['constraints']);
            } else {
                $slaveConstraints = [];
            }

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
                    $this->addConstraint($table, $newConstraint);
                }
            }
        }
    }

    protected function createTables($tables)
    {
        if (count((array)$tables) > 0) {
            foreach ($tables as $table) {
                $tablespace = $this->master['tables'][$table]['tablespace'];
                $_columns   = [];
                foreach ((array)$this->master['tables'][$table]['columns'] as $column => $columnConf) {
                    $type       = $columnConf['type'];
                    $precision  = $columnConf['precision'];
                    $nullable   = $columnConf['nullable'] ? null : ' NOT NULL';
                    $_columns[] = "{$column} {$type}" . ((!empty($precision)) ? "({$precision})" : null) . $nullable;
                }
                if (array_key_exists('constraints', $this->master['tables'][$table])) {
                    foreach ((array)$this->master['tables'][$table]['constraints'] as $constraint => $constraintInfo) {
                        $columns       = [];
                        $masterColumns = $this->master['tables'][$table]['columns'];
                        foreach ($constraintInfo['columns'] as $column) {
                            foreach ($masterColumns as $masterColumnName => $masterColumn) {
                                if ($masterColumn['order'] == $column) {
                                    $columns[] = $masterColumnName;
                                }
                            }
                        }
                        $constraintSrc = $constraintInfo['src'];
                        switch ($constraintInfo['type']) {
                            case 'UNIQUE':
                                $_columns[] = "CONSTRAINT {$constraint} UNIQUE ({$columns[0]})";
                                break;
                            case 'CHECK':
                                $_columns[] = "CONSTRAINT {$constraint} CHECK {$constraintSrc}";
                                break;
                            case 'PRIMARY KEY':
                                $columns    = implode(', ', $columns);
                                $_columns[] = "CONSTRAINT {$constraint} PRIMARY KEY ({$columns})";
                                break;
                        }
                    }
                }
                $owner   = $this->master['tables'][$table]['owner'];
                $columns = implode(",\n ", $_columns);
                $buffer  = "\nCREATE TABLE {$this->schema}.{$table}(\n {$columns}\n)";
                if (!empty($tablespace)) {
                    $buffer .= "\nTABLESPACE {$tablespace}";
                }
                $buffer .= ";";

                if ($this->settings['alter_owner'] === true) {
                    $buffer .= "\nALTER TABLE {$this->schema}.{$table} OWNER TO {$owner};";
                }

                if (array_key_exists('grants', $this->master['tables'][$table])) {
                    foreach ((array)$this->master['tables'][$table]['grants'] as $grant) {
                        if (!empty($grant) && $this->settings['alter_owner'] === true) {
                            $buffer .= "\nGRANT ALL ON TABLE {$this->schema}.{$table} TO {$grant};";
                        }
                    }
                }
                $this->diff[]                        = $buffer;
                $this->summary['tables']['create'][] = "{$this->schema}.{$table}";
            }
        }
    }

    protected function deleteTables($tables)
    {
        if (count((array)$tables) > 0) {
            foreach ($tables as $table) {
                $this->diff[]                      = "\nDROP TABLE {$this->schema}.{$table};";
                $this->summary['tables']['drop'][] = "{$this->schema}.{$table}";
            }
        }
    }

    protected function addColumns($table, $columns)
    {
        if (count((array)$columns) > 0) {
            foreach ($columns as $column) {
                $masterType                          = $this->master['tables'][$table]['columns'][$column]['type'];
                $masterPrecision                     = (!empty($this->master['tables'][$table]['columns'][$column]['precision'])) ? $this->master['tables'][$table]['columns'][$column]['precision'] : "";
                $columnDefault                       = (!empty($this->master['tables'][$table]['columns'][$column]['default'])) ? " DEFAULT " . $this->master['tables'][$table]['columns'][$column]['default'] : "";
                $nullable                            = $this->master['tables'][$table]['columns'][$column]['nullable'] ? "" : " NOT NULL";
                $masterPrecision                     = $masterPrecision == '' ? null : " ({$masterPrecision})";
                $this->diff[]                        = "ALTER TABLE {$this->schema}.{$table} ADD {$column} {$masterType}" . $masterPrecision . $columnDefault . $nullable . ";";
                $this->summary['column']['create'][] = "{$this->schema}.{$table}.{$column}";
            }
        }
    }

    protected function deleteColumns($table, $columns)
    {
        if (count((array)$columns) > 0) {
            foreach ($columns as $column) {
                $this->diff[]                      = "DELETE COLUMN {$column} TO TABLE {$table}";
                $this->summary['column']['drop'][] = "{$this->schema}.{$table} {$column}";
            }
        }
    }

    protected function alterColumn($table, $column)
    {
        $masterType                         = $this->master['tables'][$table]['columns'][$column]['type'];
        $masterPrecision                    = (!empty($this->master['tables'][$table]['columns'][$column]['precision'])) ? $this->master['tables'][$table]['columns'][$column]['precision'] : "";
        $columnDefault                      = (!empty($this->master['tables'][$table]['columns'][$column]['default'])) ? " SET DEFAULT " . $this->master['tables'][$table]['columns'][$column]['default'] : "";
        $nullable                           = $this->master['tables'][$table]['columns'][$column]['nullable'] ? "" : " SET NOT NULL";
        $masterPrecision                    = $masterPrecision == '' ? null : " ({$masterPrecision})";
        $this->diff[]                       = "ALTER TABLE {$this->schema}.{$table} ALTER {$column} TYPE {$masterType}" . $masterPrecision . $columnDefault . $nullable . ";";
        $this->summary['column']['alter'][] = "{$this->schema}.{$table} {$column}";
    }

    protected function addConstraint($table, $constraint)
    {
        $constraintData = $this->master['tables'][$table]['constraints'][$constraint];
        $type           = strtoupper($constraintData['type']);
        $columns        = [];
        $masterColumns  = $this->master['tables'][$table]['columns'];
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
                    $this->diff[] = "ALTER TABLE {$this->schema}.{$table}
                    ADD CONSTRAINT {$constraint} {$type} (" . implode(', ', $columns) . ")
                    REFERENCES {$fkSchema}.{$fkTable} (" . implode(', ', $fkColumns) . ")  MATCH {$match}
                    ON UPDATE {$updateAction} ON DELETE {$deleteAction};";
                } else {
                    $this->summary[] = 'CONSTRAINT ' . $constraint . ' FOR TABLE ' . $this->schema . '.' . $table . ' COULD NOT BE ADDED BECAUSE NO COLUMNS WERE DETECTED';
                }
                break;
        }
    }

    protected function dropConstraint($table, $constraint)
    {
        $this->diff[] = "ALTER TABLE {$this->schema}.{$table} DROP CONSTRAINT {$constraint} CASCADE;";
    }
}