            }, 
            "body": { 
                "type": "text", 
                "fielddata": True 
            }, 
            "author": { 
                "type": "text", 
                "fielddata": True 
            }, 
            "subreddit": { 
                "type": "text", 
                "fielddata": True 
            }, 
            "prediction": { 
                "type": "float" 
            }, 
            "words": { 
                "type": "text", 
                "fielddata": True 
            } 
        } 
    } 
} 
 
elastic = Elasticsearch(hosts=[elastic_host]) 
 
response = elastic.indices.create( 
    index = elastic_index, 
    body = mapping, 
    ignore = 400 
) 
 
if 'acknowledged' in response: 
    if response['acknowledged'] == True: 
        print ("INDEX MAPPING SUCCESS FOR INDEX:", response['index']) 
 
elif 'error' in response: 
        print ("ERROR:", response['error']['root_cause']) 
        print ("TYPE:", response['error']['type']) 
 
ssc.start() 
ssc.awaitTermination()
