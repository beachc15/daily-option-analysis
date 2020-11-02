#Tasks
- Manipulate Data
    - Add current price, volume, vol
    - add day trend (price volume vol)
    - 5 day trend
    - 3 month trend
    - 1 year trend
    - beta
- for valuing theoretical option prices:
    - make sure to do a dividend lookup to price them
        - this should be done with a cache
        
- backtrack lookup
    - lets just start to record all stock prices in a table
        - we will need one table for our 10-minute interval prices
        - one table for the day-accurate prices
##Fixes
- negative values of T being throw in for morning values (<8:00 AM)
    
## data types
- beta_cache

##things for rasbpi
- start storing a cache with things like daily betas
    - although it may be worht just computing beta ourself
        - test to see how to maximize accuracy and minimize time for this computation