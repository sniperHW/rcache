# rcache
a cache system for hot data。

    redis(hot data)    pgsql(cold data)
          \                /
           \              /  
            \            /   
               dataproxy
                  |
                client 



## get cache hit

    client          dataproxy           redis             pgsql      
       -----get-------> 
                      ---------hmget----->  
                      <---------value-----                  
       <-----value-----


## get cache miss

    client          dataproxy           redis             pgsql      
       -----get-------> 
                      ---------hmget----->  
                      <-------nil---------                  
                      ------query pgsql--------------------->
                      <---------value----------------------- 
                      ------hmset-------->
      <-----value-----                 

## set cache hit


    client          dataproxy           redis             pgsql      
       -----set-------> 
                      ---------hmset----->  
                      <-------ok----------                  
       <-----ok--------
                      ---------async writeback-------------->  


## set cache miss                      


    client          dataproxy           redis             pgsql      
       -----set-------> 
                      ---------hmset----->  
                      <---not in redis----                  
                      ------insert update statement-------->
                      <---------ok-------------------------- 
                      ------hmset-------->
      <-----ok---------                 

