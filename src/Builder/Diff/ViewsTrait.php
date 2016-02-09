<?php

namespace Pgdbsync\Builder\Diff;

trait ViewsTrait
{
    protected function diffViews()
    {
        $masterViews = isset($this->master['views']) ? array_keys((array)$this->master['views']) : [];
        $slaveViews  = isset($this->slave['views']) ? array_keys((array)$this->slave['views']) : [];

        // delete deleted views
        $deletedViews = array_diff($slaveViews, $masterViews);
        if (count($deletedViews) > 0) {
            $this->deleteViews($deletedViews);
        }

        // create new views
        $newViews = array_diff($masterViews, $slaveViews);
        if (count($newViews) > 0) {
            $this->createViews($newViews);
        }

        foreach ($masterViews as $view) {
            if (in_array($view, $newViews)) {
                continue;
            }

            if ($this->master['views'][$view]['definition'] !== $this->slave['views'][$view]['definition']) {
                $this->createView($view);
            }
        }
    }

    protected function deleteViews($views)
    {
        if (count((array)$views) > 0) {
            foreach ($views as $view) {
                $this->diff[]                     = "DROP VIEW {$this->schema}.{$view};";
                $this->summary['views']['drop'][] = "{$this->schema}.{$view}";
            }
        }
    }

    protected function createView($view)
    {
        $definition = $this->master['views'][$view]['definition'];
        $owner      = $this->master['views'][$view]['owner'];
        $buffer     = "\nCREATE OR REPLACE VIEW {$this->schema}.{$view} AS\n";
        $buffer .= "  " . $definition;
        if ($this->settings['alter_owner'] === true) {
            $buffer .= "\nALTER TABLE {$this->schema}.{$view} OWNER TO {$owner};";
        }
        foreach ($this->master['views'][$view]['grants'] as $grant) {
            if (!empty($grant) && $this->settings['alter_owner'] === true) {
                $buffer .= "\nGRANT ALL ON TABLE {$this->schema}.{$view} TO {$grant};";
            }
        }
        $this->diff[]                      = $buffer;
        $this->summary['view']['create'][] = "{$this->schema}.{$view}";
    }


    protected function createViews($views)
    {
        if (count((array)$views) > 0) {
            foreach ($views as $view) {
                $this->createView($view);
            }
        }
    }
}