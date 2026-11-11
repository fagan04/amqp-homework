| scenario | messages | confirmed | consumed_unique | missing | zero_loss | duplicates_removed | out_of_order | republished | producer_reconnects | nacks | consumer_failovers | switch_backs | leader_before | leader_after | election_s | publish_resume_s | consumer_detect_s | consumer_failover_s | publish_stall_s | consume_stall_s | rejoin_s | e2e_p50_ms | e2e_p99_ms | e2e_max_ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 40000 | 40000 | 40000 | 0 | True | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |  |  |  |  |  |  |  | 15.516 | 234.256 | 290.518 |
| node-crash | 128000 | 128000 | 128000 | 0 | True | 97 | 12 | 48 | 1 | 0 | 1 | 1 | rabbit1 |  |  | 0.0 | 0.17 | 0.21 | 0 | 0 | 8.5 | 11.813 | 2961.475 | 3224.668 |
| leader-crash | 128000 | 128000 | 128000 | 0 | True | 0 | 0 | 105 | 2 | 1 | 0 | 0 | rabbit1 | rabbit3 | 0.4 | 0.0 |  |  | 0 | 0 | 20.5 | 9.411 | 357.789 | 508.835 |
| partition | 160000 | 160000 | 160000 | 0 | True | 38 | 67 | 74 | 1 | 0 | 1 | 0 | rabbit1 |  |  | 0.0 | 7.54 | 0.21 | 0 | 8 | 23.3 | 7.943 | 7405 | 17382.96 |
| partition-leader | 160000 | 160000 | 160000 | 0 | True | 98 | 99 | 200 | 2 | 2 | 1 | 1 | rabbit3 | rabbit1 | 17.88 | 6.9 | 7.62 | 0.2 | 6 | 18 | 3.5 | 20.653 | 12465.418 | 18391.753 |
| quorum-loss | 120000 | 120000 | 120000 | 0 | True | 140 | 133 | 212 | 4 | 3 | 2 | 1 | rabbit1 |  |  | 31.0 | 0.19 | 0.2 | 30 | 31 | 8.3 | 7445.814 | 8358.4 | 30424.727 |
| rolling-restart | 160000 | 160000 | 160000 | 0 | True | 69 | 0 | 60 | 9 | 6 | 1 | 1 | rabbit1 | rabbit2 | 8.69 | 0.0 | 36.01 | 0.21 | 0 | 0 |  | 10.79 | 193.29 | 403.125 |
| full-restart | 20000 | 20000 | 20000 | 0 | True | 0 | 0 | 0 | 0 | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
