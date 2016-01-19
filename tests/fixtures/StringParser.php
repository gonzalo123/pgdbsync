<?php

class StringParser
{
    public static function trimLines($string)
    {
        return trim(implode(" ", array_map(function($row) { return trim($row);}, explode("\n", $string))));
    }
}