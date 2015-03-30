REGISTER /home/004/u/ux/uxr130130/pig/Question4_UDF.jar;
A = LOAD '/Fall2014_HW-3-Pig/movies_new.dat' using PigStorage('#') as (MOVIEID: chararray, TITLE: chararray, GENRE: chararray);
B = FOREACH A GENERATE TITLE, Question4UDF(GENRE);
DUMP B;