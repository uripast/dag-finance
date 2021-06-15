import yfinance as yf

aapl = yf.Ticker("aapl")
print(aapl)
"""
returns
<yfinance.Ticker object at 0x1a1715e898>
"""

# get stock info
aapl.info

"""
returns:
{
 'quoteType': 'EQUITY',
 'quoteSourceName': 'Nasdaq Real Time Price',
 'currency': 'USD',
 'shortName': 'Microsoft Corporation',
 'exchangeTimezoneName': 'America/New_York',
  ...
 'symbol': 'aapl'
}
"""

# get historical market data
aapl.history(period="max")
"""
returns:
              Open    High    Low    Close      Volume  Dividends  Splits
Date
1986-03-13    0.06    0.07    0.06    0.07  1031788800        0.0     0.0
1986-03-14    0.07    0.07    0.07    0.07   308160000        0.0     0.0
...
2019-04-15  120.94  121.58  120.57  121.05    15792600        0.0     0.0
2019-04-16  121.64  121.65  120.10  120.77    14059700        0.0     0.0
"""

# show actions (dividends, splits)
aapl.actions
"""
returns:
            Dividends  Splits
Date
1987-09-21       0.00     2.0
1990-04-16       0.00     2.0
...
2018-11-14       0.46     0.0
2019-02-20       0.46     0.0
"""

# show dividends
aapl.dividends
"""
returns:
Date
2003-02-19    0.08
2003-10-15    0.16
...
2018-11-14    0.46
2019-02-20    0.46
"""

# show splits
aapl.splits
"""
returns:
Date
1987-09-21    2.0
1990-04-16    2.0
...
1999-03-29    2.0
2003-02-18    2.0
"""