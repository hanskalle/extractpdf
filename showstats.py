__author__ = 'kalleh'

import pstats

stats = pstats.Stats('stats')
stats.sort_stats('time')
stats.print_stats(5)

print "nummer 2"

stats = pstats.Stats('stats2')
stats.sort_stats('time')
stats.print_stats(5)
