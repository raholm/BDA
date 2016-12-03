#!/bin/sh

filename=temperature-readings.csv
stations='75520|85250|85130|85390|85650|86420|85270|85280|85410|84260|86440|86130|85040|86200|86330|85180|86090|86340|86470|85450|86350|85460|86360|85220|85210|85050|85600|86370|87140|87150|85160|85490|85240|85630'
output=temperature-readings-ostergotland.csv

if [ -e "$filename" ]; then
    grep -E  $stations $filename > $output
fi
