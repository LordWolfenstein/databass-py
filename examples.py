'''
Just a small example of how Databass works. Just put a web server with 
HTTPS and some sort of security token between GenerateFeed and EatFeed
then you got working cloud synchronization.
'''
import time
import json
from databass import databass, printrows, shorten

print("Version =", databass.__version__)

config = {'user'     : 'root',
          'password' : 'aaa',
          'host'     : '1.2.4.8',
          'port'     : '1234',
          'database' : 'sandbox'}

db = databass(config)

tableconfigs = {
            "tablename1" :
            [
                {
                    "Field": "id",
                    "Type": "int(11)",
                    "Null": "NO",
                    "Key": "PRI",
                    "Default": "None",
                    "Extra": "auto_increment"
                },
                {
                    "Field": "text",
                    "Type": "text"
                }
            ],
            "tablename2" :
            [
                {
                    "Field": "id",
                    "Type": "int(11)",
                    "Null": "NO",
                    "Key": "PRI",
                    "Default": "None",
                    "Extra": "auto_increment"
                },
                {
                    "Field": "tid",
                    "Type": "double",
                    "Null": "YES",
                    "Key": "",
                    "Default": "None",
                    "Extra": ""
                }
            ]
        }

db.create(tableconfigs)
l=[]
for t in db.tables():
    l.append({t:db.info(t)})
print(json.dumps(l, indent=4))

for t in db.tables():
    print(t)
for i in range(3):
    t=time.time()
    print(db.run("INSERT INTO tablename1 (text) VALUES ('{}')".format(t)))

sql="SELECT * FROM tablename1"
test = db.run(sql)
print(test)
printrows(test)
for i in test:
    print(i)
print(json.dumps({"feeds":test}, indent=4))

info=db.info("tablename1")
printrows(info)
print(json.dumps({"tables":[{"tablename1":info}]}, indent=4))

print()
print()
print()
print()
sql="SELECT * FROM tablename1"
printrows(db.run(sql))

nydata=[]
d1={}
d1["text"]="bla"
d2={}
d2["text"]="blablabla"
nydata.append(d1)
nydata.append(d2)

db.insert("tablename1", nydata)
db.insert("tablename1", {"text":"dict"})
printrows(json.loads(json.dumps(db.run(sql), indent=4)))

#print()
#print(db.run("SHOW tables"))

condition={}
condition["text"] = "bla"

print("SELECT with dictionary condition")
printrows(db.select("tablename1"))
printrows(db.select("tablename1", condition))
printrows(db.select("tablename1", {"text":"dict", "id":"6"}))
printrows(db.select("tablename1", wherenot={"id":"4"}))

db.run("update tablename1 set text='ny text' where id='1'")
printrows(db.select("tablename1"))
db.run("update tablename1 set text='ny text' where id!='4'")
printrows(db.select("tablename1"))
db.run("update tablename1 set text='änny nyare text'")
printrows(db.select("tablename1"))
print("uppdaterar nu")
db.update("tablename1", {"text":"update() where"}, {"id":'5'})
db.update("tablename1", {"text":"where not"}, wherenot={"id":'5'})
printrows(db.select("tablename1"))

db.delete("tablename1", where={"id":'2'})
printrows(db.select("tablename1"))

db.delete("tablename1", wherenot={"id":'4'})
printrows(db.select("tablename1"))




print("\n"*5)
print("time to feed")
feed = []
feed.append(db.FeedInsert("tablename1", [{"text":"from feed", "id":200}, {"text":"from feed2", "id":300}]))
feed.append(db.FeedUpdate("tablename1", {"text":"uppdaterad"}, {"id":4}))
feed.append(db.FeedDrop("tablename2"))
db.EatFeed(db.GenerateFeed(feed))
printrows(db.select("tablename1"))
feed = []
feed.append(db.FeedDelete("tablename1", {"text":"uppdaterad"}))

feedtable = {
    "feedtable" :
    [
        {
            "Field": "id",
            "Type": "int(11)",
            "Null": "NO",
            "Key": "PRI",
            "Default": "None",
            "Extra": "auto_increment"
        },
        {
            "Field": "feed1",
            "Type": "text",
            "Null": "YES",
            "Key": "",
            "Default": "'feed 1'",
            "Extra": ""
        },
        {
            "Field": "feed2",
            "Type": "text",
            "Null": "YES",
            "Key": "",
            "Default": "'feed 2'",
            "Extra": ""
        }
    ]
}

feed.append(db.FeedCreate(feedtable))
print(db.EatFeed(db.GenerateFeed(feed)))
printrows(db.select("tablename1"))

#db.insert("feedtable", {"feed1":"ett"})
feed.append(db.FeedInsert("feedtable", {"feed2":"två"}))
feed.append(db.FeedInsert("feedtable", {"feed1":"ett"}))
print(db.EatFeed(db.GenerateFeed(feed)))
#print(db.code("feedtable"))
#print(db.select("feedtable"))
printrows(db.select("feedtable"))

add={
    "Field": "tal",
    "Type": "double",
}
drop = "feed1"
db.AlterTable("feedtable", add, drop)
db.AlterTable("feedtable", add)
drop = "feed2"
db.AlterTable("feedtable", drop=drop)
printrows(db.select("feedtable"))

feed = []
add={
    "Field": "flyt tal",
    "Type": "float",
}
alteration = db.FeedAlterTable("feedtable", add)

for i in range(3):
    add={
        "Field": "flyt tal " + str(i),
        "Type": "float",
    }
    feed.append(db.FeedAlterTable("feedtable", add))

feed.append(alteration)
print(db.EatFeed(db.GenerateFeed(feed)))

db.update("feedtable", {"flyt tal 1": 555.555}, {"id":1})

printrows(db.select("feedtable"))
print(db.code("feedtable"))
for t in db.tables():
    db.drop(t)
print("End.")

