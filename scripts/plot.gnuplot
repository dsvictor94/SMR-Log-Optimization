set key inside right top  vertical Right noreverse enhanced autotitles box linetype -1 linewidth 1.000

set grid

set xlabel "Throughput (kCommands/s)"
set ylabel "Latency (ms)"

set style line 1 lc rgb 'blue' lt 1 lw 2 ps 1 pt 5;
set style line 2 lc rgb 'dark-red' lt 2 lw 2 ps 1 pt 7;
set style line 3 lc rgb 'dark-green' lt 3 lw 2 ps 1 pt 9;
set style line 4 lc rgb 'orange' lt 4 lw 2 ps 1 pt 11;
set style line 5 lc rgb 'dark-pink' lt 5 lw 2 ps 1 pt 13;
set style line 6 lc rgb 'dark-violet' lt 5 lw 2 ps 1 pt 13;
set style line 7 lc rgb 'gray-90' lt 5 lw 2 ps 1 pt 13;

plot for [i=1:words(datafiles)] word(datafiles, i) using ($13/1000):($14/1000000) with lines ls 1 title "Prototype #".i


# set yrange [0:3.0e+07]
#plot "p0.tsv" using ($1-P0_min):2 with lines lw 1 lt 1 title 'prototypo-0', \
#     "p1.tsv" using ($1-P1_min):2 with lines lw 1 lt 3 title 'prototype-1'

