# Databass
Databass is a interface meant to simplyfy database transactions in Python 
by letting you use dictionaries and list of dictionaries for your transactions
instead of writing pure SQL-code. Thus easily letting you generate and read 
JSON-feeds. Preferbly over HTTPS.

The syntax for doing operations and generating feeds is identical. And the 
result from reading the feed is identical to as if the operation were done
locally.

(Only tested with Python 3. (Automatic JOIN with dictionaries is on the TODO 
list. But you can still write your own SQL and get the list of dictionaries 
from joined SELECTS.))

### Name

Databass is a punmanteau from "data" and "bass" because bass sound like base.
And you can feed the bass. Bass feed is much funnier than Atom feed.

![A picture of a data bass.](https://raw.githubusercontent.com/LordWolfenstein/databass/master/databass.png)
