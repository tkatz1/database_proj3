import pymongo
from pymongo import MongoClient
import certifi
import pprint
#Connecting to db/collection
client = MongoClient("mongodb+srv://tyrock09:baseball09@cluster0.dwhkc.mongodb.net/Fangraphs_429?retryWrites=true&w=majority",tlsCAFile=certifi.where())
db = client.Fangraphs_429
collection = db.Real
def homers():
    #Aggregation to get average barrel rate
    barrel = collection.aggregate([
    {
        "$group":
        {
        "_id": 0,
        "Average": {
        "$avg": "$Barrels"
        }
        
    }}
    ])
    #Aggregation to get average hardhit rate
    hardhit = collection.aggregate([
    {
        "$group":
        {
        "_id": 0,
        "Average": {
        "$avg": "$HardHit"
        }
        
    }}
    ])
    #Aggregation to get average home runs
    homeruns = collection.aggregate([
    {
        "$group":
        {
        "_id": 0,
        "Average": {
        "$avg": "$HR"
        }
        
    }}
    ])
    #Agregation to get average launch angle
    la = collection.aggregate([
    {
        "$group":
        {
        "_id": 0,
        "Average": {
        "$avg": "$LA"
        }
        
    }}
    ])
    barrel_avg = None
    hardhit_avg = None
    homers_avg = None
    la_avg = None
    #Saves barrel avg
    for i in barrel:   
        barrel_avg = i["Average"]
    #Saves hardhit avg
    for j in hardhit:   
        hardhit_avg = j["Average"]
    #Saves homeruns avg
    for k in homeruns:
        homers_avg = k['Average']
    #Saves average launch angle
    for l in la:
        la_avg = l['Average']
    #Finds players who are above average Barrels and HardHit and Launch Angle but below average home run hitters
    query1 = {"Barrels" : {"$gt" : barrel_avg}}
    query2 = {"HardHit" : {"$gt" : hardhit_avg}}
    query3 = {"HR" : {"$lt" : homers_avg}}
    query4 = {"LA" : {"$gt" : la_avg}}
    #Checks to see what user wants to see
    print("What do you want to see?")
    print("1. Players who are below the league standard that should improve")
    print("2. Top expected Home Run Producers")
    q = int(input(""))
    if q == 1:
        #loads queries to the collection
        temp = collection.find({"$and": [query1, query2, query3, query4]})
        #Prints out players
        for b in temp:
            print(b['Name']+'\n'+"Barrels: "+str(b['Barrels'])+'\t'+'HardHit: '+str(b['HardHit'])+'\t'+"LA: "+str(b['LA'])+'\t'+"Actual HR Total: " + str(b['HR']))
    elif q == 2:
        temp_collection = collection.aggregate(  [
        {
            #projects the correct information to view/calculate
            '$project': {
                '_id': -0, 
                'Barrels': 1,
                'HardHit': 1,
                'HR' : 1, 
                'LA': 1,
                'Name': 1
            }
        }, 
        #adds the home run production category
            {'$addFields':{
                'HomeRunProduction':{
                    "$add": [
                        {'$divide':[{'$subtract':["$Barrels", barrel_avg]},barrel_avg]},
                        {'$divide':[{'$subtract': ["$HardHit", hardhit_avg]},hardhit_avg]},
                        {'$divide':[{'$subtract': ["$LA", la_avg]},la_avg]}
                    ]
                }

            }},
            #sorts it from high to low
            {'$sort':
            {
                'HomeRunProduction':-1
                }
            },
            {#shows top 10
                '$limit':10
            }
        ])

        #prints to terminal
        for var in temp_collection:
            print(var)


def batting():
    #checks what the user wants to see
    print("What would you like to see:")
    print("1. Complete list of players who have a higher expected batting average:")
    print("2. Top 10")
    temp_input = int(input(" "))
    if temp_input == 1:
        #gets all players that have a greater expected batting average compared to their actual batting average
        temp = collection.find({"$expr": {"$gt":["$xBA","$AVG"]}})
        #formats/prints out all players from query
        for w in temp:
            print(w['Name'] + '\t' + "xBA: " + str(w['xBA']) + '\t' + "BA: " + str(w['AVG']) )
    elif temp_input == 2:
        temp_collection = collection.aggregate(   [
    {
        #gets all stats needed to print out/use for calculations
        '$project': {
            '_id': -0, 
            'xBA': 1, 
            'AVG': 1,
            'HardHit' : 1, 
            'Name': 1
        }
        }, {
        #gets the difference between expected batting average and real batting average
        '$addFields': {
            'AVG_Room_for_Improvement': {
                '$subtract': [
                    '$xBA', '$AVG'
                ]
            }
        }
     }, {
        #sorts from high to low
        '$sort': {
            'AVG_Room_for_Improvement': -1
        }
        }, {
        #gets top 10
        '$limit': 10
        }
        ])
        hardhit = collection.aggregate([
        {
            "$group":
            {
            "_id": 0,
            "Average": {
            "$avg": "$HardHit"
            }
            
        }}
        ])
        ba = collection.aggregate([
        {
            "$group":
            {
            "_id": 0,
            "BA": {
            "$avg": "$AVG"
            }
            
        }}
        ])
        for t in hardhit:
            print("HardHit average: " + str(t['Average']))
        for g in ba:
            print("Average AVG: " + str(g['BA']))
            #prints everything from collection out
        for i in temp_collection:
            print(i)


def value():
    #checks to see what users are looking to see
    print("1. All high production hitters")
    print("2. Top 10")
    ask = int(input(""))
    coll = collection.aggregate([
        #gets the avergae wRC+ for hitters in collection
    {
        '$group': {
            '_id': 0, 
            'AveragewRC': {
                '$avg': '$wRC+'
            }
        }
    }
])
    coll2 = collection.aggregate([
        #gets the avergae woBA for hitters in collection
        {
            '$group': {
                '_id': 0, 
                'AveragewoBA': {
                    '$avg': '$wOBA'
                }
            }
        }
    ])
    average_woBA = None
    average_wrc = None
    for i in coll:   
        #saves average wRC+
        average_wrc = i["AveragewRC"]
    for j in coll2:  
         #saves average woBA 
        average_woBA = j["AveragewoBA"]
    if ask == 1:
        coll3 = collection.aggregate([
        {
            #gets all the information needed to calculate/print
            '$project': {
                '_id': -0, 
                'Name': 1,
                'wOBA': 1,
                'wRC+': 1
            }
        },{ 
            #adds field to get hitters who excell in wRC+ and woBA
            '$addFields':{
                'newStat': 
                {"$add" : [
                {"$divide" : [{"$subtract" : ["$wRC+", average_wrc]}, average_wrc]}
            ,
                {"$divide" : [
            {"$subtract" : ["$wOBA", average_woBA]}
                , average_woBA]}
                ]}
            }
        },
        {
            #makes sure that players are above average in woBA
            '$match':{
                "wOBA":{"$gt": average_woBA}
            }
        },
        {
            #makes sure that players are above average in wRC+
            '$match':{
                "wRC+":{"$gt": average_wrc}
            }
        }
        
        
        ])
    elif ask == 2:
        #does same as above with sorting and limit
         coll3 = collection.aggregate([
        {
            '$project': {
                '_id': -0, 
                'Name': 1,
                'wOBA': 1,
                'wRC+': 1
            }
        },{ 
            '$addFields':{
                'ProductionLevel': 
                {"$add" : [
                {"$divide" : [{"$subtract" : ["$wRC+", average_wrc]}, average_wrc]}
            ,
                {"$divide" : [
            {"$subtract" : ["$wOBA", average_woBA]}
                , average_woBA]}
                ]}
            }
        },
        {
            '$match':{
                "wOBA":{"$gt": average_woBA}
            }
        },
        {
            '$match':{
                "wRC+":{"$gt": average_wrc}
            }
        },
        {
            #sorts by ProductionLevel (high to low)
            "$sort":{
                'ProductionLevel':-1
            }
        },
        {
            #gets top 10
            "$limit": 10
        }
        
        
        ])

    for k in coll3:
        #prints out
        print(k)

def onbase():
    coll = collection.aggregate([
    {
        #gets the avergae K%
        '$group': {
            '_id': 0, 
            'AverageK': {
                '$avg': '$K%'
            }
        }
    }
    ])
    coll2 = collection.aggregate([
        #Gets average BB%
        {
            '$group': {
                '_id': 0, 
                'AverageBB': {
                    '$avg': '$BB%'
                }
            }
        }
    ])
    coll3 = collection.aggregate([
        #gets average OBP
        {
            '$group': {
                '_id': 0, 
                'AverageOBP': {
                    '$avg': '$OBP'
                }
            }
        }
    ])
    coll5 = collection.aggregate([
        #Gets average BABIP
        {
            '$group': {
                '_id': 0, 
                'AverageBABIP': {
                    '$avg': '$BABIP'
                }
            }
        }
    ])

    average_K = None
    average_BB = None
    average_OBP = None
    average_BABIP = None
    for i in coll:   
        #saves average K%
        average_K = i["AverageK"]
    for j in coll2:
          #saves average BB%   
        average_BB = j["AverageBB"]  
    for k in coll3:   
        #saves average OBP
        average_OBP = k["AverageOBP"]
    for u in coll5:
        #saves average BABIP
        average_BABIP = u['AverageBABIP']
    coll4 = collection.aggregate([
    {
        #gets all the information needed to print and calculate
        '$project': {
            '_id': 0, 
            'Name': 1, 
            'K%': 1, 
            'BB%': 1, 
            'BABIP': 1,
            'OBP' : 1, 
            'AVG': 1
        }
    }, {
        #gets all BABIP less than average
        '$match': {
            'BABIP': {
                '$lt': average_BABIP
            }
        }
    },
    {
        #gets all K% less than average
        '$match': {
            'K%': {
                '$lt': average_K
            }
        }
    },
    {
        #Gets all BB% greater than average
        '$match': {
            'BB%': {
                '$gt': average_BB
            }
        }
    },
        {
            #Gets all OBP less than average
        '$match': {
            'OBP': {
                '$lt': average_OBP
            }
        }
        
    },
  {
      #adds field onBaseValue
      '$addFields':{
          'OnBaseValue':{
              #adds up following values
             "$add" : [
                 #gets K% relative to league
                {"$divide" : [{"$subtract" : [average_K, "$K%"]}, average_K]}
            ,
               #gets BB% relative to league
                {"$divide" : [
            {"$subtract" : ["$BB%", average_BB]}
                , average_BB]},
                #gets OBP relative to league
                {"$divide" : [{"$subtract" : ["$OBP", average_OBP]}, average_OBP]}
            ] 
          }

      }

  },
  {
      #sorts greatest to least
      '$sort':{
          'OnBaseValue':-1
      }
  }


    

])
#prints out
    for w in coll4:
        print(w)
def main():
    print("What do you want to see?: ")
    print("1. Most valuable producing hitters")
    print("2. See players who should hit more homeruns")
    print("3. See players who should get more hits")
    print("4. Players who should get on base")
    print("5. Look at player statistics")
    input_var = int(input(" "))
    if input_var == 2:
        homers()
    if input_var == 3:
        batting()
    if input_var == 1:
        value()  
    if input_var == 4:
        onbase()
    if input_var == 5:
        inp = input("Enter Name: ")
        temp = collection.find({"Name" : inp})
        for y in temp:
            print(y)    
main()

