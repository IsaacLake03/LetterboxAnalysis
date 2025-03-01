# Week 2 of personal analysis, cleaning and fixing this shiz up my guy

## Issues

For me cleaning the data took literally no work, since the data is already fairly clean
I ultimately just ran it through duckDB's csv to parquet command to quickly change all
the files over. In the process I wrote a script that converts all the CSV in one folder
to parquets in an output folder.

The main Issues I had was in my query creation for testing. I made some simple mistakes
when it came to defining the path to the parquet files and the error I got returned was
this file has no magic bytes at the end, so I assumed that the conversion had failed. 
Practically after an hour or so of manually converting csv's to parquets and getting the
same errors (as well as some surprising segfaults). I realized that the path was wrong
once I fixed the path my queries started working perfectly.

I also ran into some issues with convering the countries file automatically, so I wrote
a quick polars thing to manually complete that conversion.

On the analysis side my main issue is narrowing down to movies, the database itself also
includes tvshows music videos and short films, so I set a time range of 60-240 minutes 
and noticed that shows were not in the releases db so I checked that the movies had a
release date.

I am starting to realize that this data set has some intrinsic biases, especially towards
things like Anime Movies that would only be watched by fans of the show.