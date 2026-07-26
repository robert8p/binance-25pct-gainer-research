# Reference execution specification for the 25% fork

The execution engine has been converted to a profit target of **125% of the previous split-adjusted regular-session close**. Other source assumptions remain: US$500 requested position, five-second reaction delay, displayed NBBO capacity, five basis points slippage each side, 5% stop, maximum five selections per strategy per day, and an executable-bid time exit five minutes before close.

This specification is not active because the inherited signal rules were derived from the 50% cohort. `ENABLE_BACKTEST_STAGE=false` is binding until 25%-specific signals are frozen and approved.
