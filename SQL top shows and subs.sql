use shows_tb;
Select title, subscription 
from netflix 
inner join imdb_1000
on netflix.title = imdb_1000.Series_Title
union
Select title, subscription 
from hulu 
inner join imdb_1000
on hulu.title = imdb_1000.Series_Title
union
Select title, subscription 
from amazon_prime 
inner join imdb_1000
on amazon_prime.title = imdb_1000.Series_Title
union
Select title, subscription 
from disney_plus 
inner join imdb_1000
on disney_plus.title = imdb_1000.Series_Title

