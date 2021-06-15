{    
    "name" : "fact",    
    "source": {      
         "sql" : "sql/fact_table.sql",       
          "dependencies": [          
                            { "db": "stocks",
                              "table": "raw_fact_table",
                            }
                           ],
         "schedule_interval": "@hourly",        
         "start_date": "2018-01-01"    
     }  
}