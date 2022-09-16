DROP TABLE iF EXISTS table_probe;
CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e BOOLEAN);
INSERT INTO table_probe VALUES 
 (null,null,13.579426,17.395742,cast(1 as BOOLEAN)), 
 (4,null,null,null,cast(1 as BOOLEAN)), 
 (null,3,4.677344,0.230301,cast(0 as BOOLEAN)), 
 (null,4,null,null,cast(0 as BOOLEAN)), 
 (2,4,12.236094,null,null), 
 (null,null,null,7.451489,cast(0 as BOOLEAN)), 
 (null,13,1.093972,null,null), 
 (null,null,17.327841,null,cast(0 as BOOLEAN)), 
 (null,null,null,null,null), 
 (null,null,7.563405,null,cast(0 as BOOLEAN)), 
 (7,null,4.425583,null,cast(0 as BOOLEAN)), 
 (null,8,3.919434,null,null), 
 (null,null,15.175970,null,cast(0 as BOOLEAN)), 
 (7,6,12.095802,10.907915,null), 
 (null,null,9.590337,10.943810,null), 
 (17,null,null,9.381378,cast(0 as BOOLEAN)), 
 (null,10,0.489520,null,null), 
 (null,null,18.815826,9.387256,cast(0 as BOOLEAN)), 
 (6,null,18.015230,9.466807,null), 
 (18,null,10.863560,null,cast(0 as BOOLEAN)), 
 (null,1,null,null,cast(0 as BOOLEAN)), 
 (16,11,19.956015,10.546609,cast(1 as BOOLEAN)), 
 (null,null,null,null,cast(1 as BOOLEAN)), 
 (null,null,5.179866,0.711589,null), 
 (null,null,2.879229,null,cast(0 as BOOLEAN)), 
 (16,10,11.319838,null,null), 
 (4,null,null,11.936088,cast(0 as BOOLEAN)), 
 (2,6,null,12.890524,null), 
 (null,0,null,null,null), 
 (null,4,9.249078,null,null), 
 (6,null,9.860779,null,null), 
 (null,null,null,18.697695,cast(1 as BOOLEAN)), 
 (12,null,19.190641,null,null), 
 (null,2,null,null,cast(1 as BOOLEAN)), 
 (15,null,7.816310,null,cast(1 as BOOLEAN)), 
 (null,13,12.547574,11.056221,null), 
 (18,null,null,9.454512,cast(1 as BOOLEAN)), 
 (null,9,null,null,null), 
 (17,null,null,null,null), 
 (null,null,null,null,null), 
 (14,null,null,null,null), 
 (null,7,null,null,cast(0 as BOOLEAN)), 
 (14,null,7.489988,null,null), 
 (null,null,19.923798,5.835182,cast(0 as BOOLEAN)), 
 (null,0,5.536547,null,null), 
 (null,10,null,10.369953,null), 
 (4,6,null,12.425122,null), 
 (18,5,null,12.822274,null), 
 (1,null,null,7.922956,cast(0 as BOOLEAN)), 
 (null,null,13.120583,12.764483,cast(1 as BOOLEAN)), 
 (null,3,13.750521,15.825510,null), 
 (null,9,3.579504,3.692991,cast(1 as BOOLEAN)), 
 (18,null,14.185926,null,cast(0 as BOOLEAN)), 
 (10,null,null,null,null), 
 (null,null,null,13.876410,null), 
 (null,20,5.168427,10.689203,null), 
 (13,9,null,8.044952,cast(1 as BOOLEAN)), 
 (null,8,null,null,cast(0 as BOOLEAN)), 
 (7,12,2.874648,18.010923,null), 
 (7,9,null,13.996257,null), 
 (null,null,null,5.110101,cast(1 as BOOLEAN)), 
 (0,null,null,null,cast(1 as BOOLEAN)), 
 (15,20,null,2.433366,cast(1 as BOOLEAN)), 
 (null,null,6.877327,null,null), 
 (null,4,null,null,cast(0 as BOOLEAN)), 
 (null,null,null,1.371292,cast(0 as BOOLEAN)), 
 (null,2,10.193166,6.558298,cast(1 as BOOLEAN)), 
 (null,null,10.034531,10.936283,null), 
 (null,6,6.655898,null,null), 
 (19,17,19.211887,null,null), 
 (4,null,null,12.246995,null), 
 (null,null,3.251728,null,null), 
 (null,null,null,null,null), 
 (null,1,4.028035,1.541852,cast(1 as BOOLEAN)), 
 (3,null,17.243458,null,cast(0 as BOOLEAN)), 
 (4,5,19.893991,6.570177,null), 
 (null,null,null,8.343103,null), 
 (17,null,13.249291,null,null), 
 (null,5,5.557268,null,cast(1 as BOOLEAN)), 
 (1,null,null,16.358362,cast(0 as BOOLEAN)), 
 (4,null,null,null,cast(1 as BOOLEAN)), 
 (4,4,5.679892,4.748672,null), 
 (10,15,null,null,cast(1 as BOOLEAN)), 
 (null,6,8.412988,3.148334,cast(1 as BOOLEAN)), 
 (10,7,null,8.768213,cast(1 as BOOLEAN)), 
 (null,8,15.749293,8.949374,null), 
 (null,10,null,10.657312,null), 
 (null,null,null,15.862835,null), 
 (null,null,13.071738,2.885325,cast(1 as BOOLEAN)), 
 (2,null,null,15.625362,cast(1 as BOOLEAN)), 
 (4,null,8.637752,null,cast(1 as BOOLEAN)), 
 (13,null,null,null,cast(1 as BOOLEAN)), 
 (16,2,16.020409,null,cast(0 as BOOLEAN)), 
 (null,0,10.035229,11.090077,cast(1 as BOOLEAN)), 
 (7,null,7.010090,null,null), 
 (8,null,null,12.584488,null), 
 (null,0,null,13.995894,null), 
 (null,null,17.535728,null,cast(1 as BOOLEAN)), 
 (0,null,19.240578,null,null), 
 (null,null,null,16.119673,cast(1 as BOOLEAN)) 
;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a < r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b < r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_c < r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_d < r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a < r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b < r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_c < r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_d < r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a > r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b > r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_c > r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_d > r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a > r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b > r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_c > r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_d > r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a <= r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b <= r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_c <= r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_d <= r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a <= r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b <= r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_c <= r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_d <= r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a >= r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b >= r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_c >= r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_d >= r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a >= r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b >= r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_c >= r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_d >= r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a <> r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b <> r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_c <> r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_d <> r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a <> r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b <> r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_c <> r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_d <> r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
DROP TABLE iF EXISTS table_probe;
CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e BOOLEAN);
INSERT INTO table_probe VALUES 
 (null,null,13.952501,4.530655,null), 
 (null,null,15.803839,null,cast(0 as BOOLEAN)), 
 (15,null,null,12.659693,null), 
 (1,17,4.037114,4.842569,cast(1 as BOOLEAN)), 
 (null,null,12.625079,8.975976,null), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (11,19,null,5.105775,null), 
 (2,8,9.501653,null,null), 
 (null,null,null,null,null), 
 (null,4,null,5.345038,cast(1 as BOOLEAN)), 
 (4,null,null,15.122752,null), 
 (3,11,null,null,cast(0 as BOOLEAN)), 
 (null,null,1.441631,16.794441,null), 
 (1,0,5.178024,15.077592,cast(1 as BOOLEAN)), 
 (7,null,12.460470,5.607733,cast(0 as BOOLEAN)), 
 (4,11,null,3.261966,cast(1 as BOOLEAN)), 
 (8,5,null,null,cast(0 as BOOLEAN)), 
 (16,3,6.154616,18.281067,cast(0 as BOOLEAN)), 
 (2,19,null,6.234939,cast(1 as BOOLEAN)), 
 (7,null,17.725491,9.768404,null), 
 (null,null,null,1.947052,cast(0 as BOOLEAN)), 
 (null,9,null,17.160421,cast(1 as BOOLEAN)), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (null,null,8.028561,13.661715,null), 
 (17,18,null,5.919156,cast(1 as BOOLEAN)), 
 (null,null,null,null,null), 
 (null,null,null,11.202946,null), 
 (20,null,null,3.855041,cast(1 as BOOLEAN)), 
 (18,10,null,null,null), 
 (6,null,8.079187,null,cast(0 as BOOLEAN)), 
 (14,2,null,1.843845,null), 
 (19,null,null,null,cast(1 as BOOLEAN)), 
 (1,null,11.371578,12.794939,null), 
 (13,2,1.605743,17.904762,cast(0 as BOOLEAN)), 
 (9,16,null,0.008931,cast(1 as BOOLEAN)), 
 (18,null,null,19.641541,cast(0 as BOOLEAN)), 
 (14,null,18.313583,null,cast(1 as BOOLEAN)), 
 (null,4,10.893944,6.596070,cast(1 as BOOLEAN)), 
 (null,null,10.033355,null,cast(0 as BOOLEAN)), 
 (9,12,null,18.461605,cast(1 as BOOLEAN)), 
 (15,null,null,null,cast(0 as BOOLEAN)), 
 (null,null,null,null,cast(1 as BOOLEAN)), 
 (10,null,7.345752,2.146721,cast(0 as BOOLEAN)), 
 (4,10,14.944501,0.765588,cast(0 as BOOLEAN)), 
 (null,null,11.579693,1.246045,null), 
 (null,15,null,null,cast(1 as BOOLEAN)), 
 (null,null,1.250635,1.111837,null), 
 (16,null,2.476204,null,cast(0 as BOOLEAN)), 
 (null,null,12.843645,null,null), 
 (5,16,6.609429,null,null), 
 (null,2,null,null,null), 
 (null,null,19.975851,null,cast(1 as BOOLEAN)), 
 (10,6,null,null,null), 
 (9,null,null,17.808403,null), 
 (20,null,null,2.426910,null), 
 (null,14,17.119173,null,cast(0 as BOOLEAN)), 
 (4,14,3.487971,null,null), 
 (19,null,2.825229,null,null), 
 (11,null,12.132398,17.625626,null), 
 (null,null,null,17.383488,null), 
 (null,null,11.500883,14.590654,null), 
 (13,null,null,null,cast(0 as BOOLEAN)), 
 (19,null,2.818234,11.237744,null), 
 (null,null,null,null,null), 
 (null,null,5.597532,null,null), 
 (null,7,null,null,null), 
 (15,18,null,null,null), 
 (null,null,9.163226,null,null), 
 (null,null,9.975143,13.704467,null), 
 (null,7,null,null,null), 
 (8,14,null,8.092579,cast(0 as BOOLEAN)), 
 (null,null,17.990070,4.459930,null), 
 (15,19,null,5.966661,cast(1 as BOOLEAN)), 
 (null,null,19.893452,6.516940,null), 
 (12,null,1.396299,2.996652,null), 
 (13,0,null,18.554848,null), 
 (7,null,11.230076,null,cast(0 as BOOLEAN)), 
 (3,5,9.465509,null,null), 
 (8,2,15.281605,14.179075,null), 
 (14,10,null,null,null), 
 (null,11,15.695629,null,null), 
 (16,8,null,4.067866,null), 
 (1,null,null,null,cast(1 as BOOLEAN)), 
 (null,null,null,null,cast(1 as BOOLEAN)), 
 (null,null,null,null,null), 
 (12,14,16.012121,1.800750,cast(1 as BOOLEAN)), 
 (1,3,12.595507,12.860389,null), 
 (2,15,null,null,null), 
 (0,null,null,8.650033,cast(1 as BOOLEAN)), 
 (3,null,1.143209,null,null), 
 (null,null,null,null,null), 
 (null,null,17.916868,null,cast(0 as BOOLEAN)), 
 (14,1,null,5.926614,null), 
 (10,null,null,null,null), 
 (16,null,0.171307,6.264589,null), 
 (8,null,18.455881,13.047772,cast(1 as BOOLEAN)), 
 (null,null,null,18.582405,cast(0 as BOOLEAN)), 
 (12,1,5.167833,15.246730,null), 
 (17,null,4.872612,0.805916,cast(1 as BOOLEAN)), 
 (4,12,null,9.662813,cast(1 as BOOLEAN)) 
;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe JOIN table_hash ON l_a < r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe JOIN table_hash ON l_b < r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe JOIN table_hash ON l_c < r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe JOIN table_hash ON l_d < r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe LEFT JOIN table_hash ON l_a < r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe LEFT JOIN table_hash ON l_b < r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe LEFT JOIN table_hash ON l_c < r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe LEFT JOIN table_hash ON l_d < r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_b) from table_probe JOIN table_hash ON l_a > r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_b) from table_probe JOIN table_hash ON l_b > r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_b) from table_probe JOIN table_hash ON l_c > r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_b) from table_probe JOIN table_hash ON l_d > r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_b) from table_probe LEFT JOIN table_hash ON l_a > r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_b) from table_probe LEFT JOIN table_hash ON l_b > r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_b) from table_probe LEFT JOIN table_hash ON l_c > r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_b) from table_probe LEFT JOIN table_hash ON l_d > r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(r_a) from table_probe JOIN table_hash ON l_a <= r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(r_a) from table_probe JOIN table_hash ON l_b <= r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(r_a) from table_probe JOIN table_hash ON l_c <= r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(r_a) from table_probe JOIN table_hash ON l_d <= r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(r_a) from table_probe LEFT JOIN table_hash ON l_a <= r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(r_a) from table_probe LEFT JOIN table_hash ON l_b <= r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(r_a) from table_probe LEFT JOIN table_hash ON l_c <= r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(r_a) from table_probe LEFT JOIN table_hash ON l_d <= r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(r_b) from table_probe JOIN table_hash ON l_a >= r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(r_b) from table_probe JOIN table_hash ON l_b >= r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(r_b) from table_probe JOIN table_hash ON l_c >= r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(r_b) from table_probe JOIN table_hash ON l_d >= r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(r_b) from table_probe LEFT JOIN table_hash ON l_a >= r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(r_b) from table_probe LEFT JOIN table_hash ON l_b >= r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(r_b) from table_probe LEFT JOIN table_hash ON l_c >= r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(r_b) from table_probe LEFT JOIN table_hash ON l_d >= r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_c) from table_probe JOIN table_hash ON l_a <> r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_c) from table_probe JOIN table_hash ON l_b <> r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_c) from table_probe JOIN table_hash ON l_c <> r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_c) from table_probe JOIN table_hash ON l_d <> r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_c) from table_probe LEFT JOIN table_hash ON l_a <> r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_c) from table_probe LEFT JOIN table_hash ON l_b <> r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_c) from table_probe LEFT JOIN table_hash ON l_c <> r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_c) from table_probe LEFT JOIN table_hash ON l_d <> r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
DROP TABLE iF EXISTS table_probe;
CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e BOOLEAN);
INSERT INTO table_probe VALUES 
 (null,1,null,null,cast(0 as BOOLEAN)), 
 (18,20,10.973284,1.792094,cast(1 as BOOLEAN)), 
 (7,null,null,null,null), 
 (null,8,4.178007,null,cast(1 as BOOLEAN)), 
 (17,8,18.640579,null,null), 
 (null,2,14.407192,null,cast(1 as BOOLEAN)), 
 (18,null,9.217609,5.743506,cast(0 as BOOLEAN)), 
 (null,10,11.403898,null,cast(1 as BOOLEAN)), 
 (null,4,6.592628,18.072235,cast(0 as BOOLEAN)), 
 (0,null,null,null,cast(0 as BOOLEAN)), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (14,0,null,null,cast(1 as BOOLEAN)), 
 (null,null,null,null,cast(1 as BOOLEAN)), 
 (null,7,null,17.497810,cast(1 as BOOLEAN)), 
 (null,null,null,3.182853,null), 
 (null,null,null,null,null), 
 (null,null,19.862217,4.798660,null), 
 (11,null,17.545179,null,null), 
 (null,0,null,16.612234,null), 
 (null,null,9.047435,7.423200,null), 
 (13,null,2.574424,null,cast(0 as BOOLEAN)), 
 (null,7,8.101181,null,cast(1 as BOOLEAN)), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (null,14,5.591409,14.362763,cast(1 as BOOLEAN)), 
 (16,18,17.602011,15.057251,null), 
 (null,null,null,15.357432,cast(0 as BOOLEAN)), 
 (9,17,9.331633,null,null), 
 (null,19,0.050701,null,cast(0 as BOOLEAN)), 
 (null,null,8.026328,null,cast(1 as BOOLEAN)), 
 (null,3,15.577219,null,cast(0 as BOOLEAN)), 
 (5,null,null,5.155118,cast(1 as BOOLEAN)), 
 (18,0,null,null,null), 
 (1,17,17.697208,null,null), 
 (14,18,null,5.558038,cast(1 as BOOLEAN)), 
 (16,null,null,17.042759,null), 
 (null,19,null,2.006272,null), 
 (null,10,17.375418,10.525501,cast(0 as BOOLEAN)), 
 (null,15,null,null,cast(0 as BOOLEAN)), 
 (15,18,null,7.307696,null), 
 (15,null,16.203339,11.195553,null), 
 (13,9,null,15.110970,cast(0 as BOOLEAN)), 
 (null,2,19.321051,null,null), 
 (15,null,8.929798,11.874270,cast(1 as BOOLEAN)), 
 (19,0,10.379757,null,null), 
 (null,null,null,5.750460,cast(1 as BOOLEAN)), 
 (5,null,null,3.946940,null), 
 (null,16,null,13.683069,cast(0 as BOOLEAN)), 
 (null,6,null,7.575488,cast(1 as BOOLEAN)), 
 (null,null,13.705090,19.664261,null), 
 (null,18,14.421682,null,null), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (null,null,2.768919,13.130244,null), 
 (19,null,16.347454,15.480739,null), 
 (5,10,16.888840,null,null), 
 (null,null,null,8.751291,cast(0 as BOOLEAN)), 
 (11,3,13.994781,null,null), 
 (19,null,null,1.033004,cast(0 as BOOLEAN)), 
 (12,6,null,0.123920,null), 
 (null,9,12.753991,null,cast(0 as BOOLEAN)), 
 (3,null,null,null,null), 
 (null,11,8.659972,0.447732,null), 
 (null,null,null,null,cast(1 as BOOLEAN)), 
 (9,null,14.140367,null,null), 
 (4,7,14.438519,null,cast(0 as BOOLEAN)), 
 (10,null,13.528852,5.298341,null), 
 (null,10,null,null,cast(1 as BOOLEAN)), 
 (8,null,8.456336,15.014649,null), 
 (15,5,15.684481,null,null), 
 (13,7,null,null,null), 
 (3,11,15.296356,12.157665,cast(0 as BOOLEAN)), 
 (null,null,null,2.977417,cast(1 as BOOLEAN)), 
 (1,2,null,3.490061,null), 
 (13,null,null,null,null), 
 (17,null,19.584225,null,null), 
 (null,0,6.380630,null,null), 
 (9,10,null,8.664831,null), 
 (null,19,null,null,null), 
 (19,null,8.672592,4.129810,null), 
 (7,null,7.024665,14.218216,null), 
 (17,null,null,3.428082,null), 
 (4,4,10.583395,null,cast(1 as BOOLEAN)), 
 (2,11,null,null,cast(1 as BOOLEAN)), 
 (null,2,null,15.509794,null), 
 (16,null,19.001616,15.221654,cast(1 as BOOLEAN)), 
 (3,null,17.733500,18.180426,null), 
 (null,12,null,null,null), 
 (17,8,1.353500,null,null), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (null,8,null,null,null), 
 (15,null,null,null,null), 
 (null,0,null,4.821831,null), 
 (4,null,null,5.800940,cast(0 as BOOLEAN)), 
 (6,null,6.044622,null,cast(0 as BOOLEAN)), 
 (9,null,0.155688,4.404496,null), 
 (16,null,12.155382,null,cast(0 as BOOLEAN)), 
 (null,1,null,14.128471,cast(0 as BOOLEAN)), 
 (null,null,null,null,null), 
 (12,12,null,18.449457,cast(1 as BOOLEAN)), 
 (null,10,16.146454,null,cast(1 as BOOLEAN)), 
 (null,null,16.057093,null,cast(0 as BOOLEAN)) 
;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a +1 < r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b +1 < r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_c +1 < r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_d +1 < r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a +1 < r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b +1 < r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_c +1 < r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_d +1 < r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a > 1 + r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b > 1 + r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_c > 1 + r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_d > 1 + r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a > 1 + r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b > 1 + r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_c > 1 + r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_d > 1 + r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a -1 < 1 + r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b -1 < 1 + r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_c -1 < 1 + r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_d -1 < 1 + r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a -1 < 1 + r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b -1 < 1 + r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_c -1 < 1 + r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_d -1 < 1 + r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a *2 <= r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b *2 <= r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_c *2 <= r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_d *2 <= r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a *2 <= r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b *2 <= r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_c *2 <= r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_d *2 <= r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a >= 2 * r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b >= 2 * r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_c >= 2 * r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_d >= 2 * r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a >= 2 * r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b >= 2 * r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_c >= 2 * r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_d >= 2 * r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a *2 <= 2 * r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b *2 <= 2 * r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_c *2 <= 2 * r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_d *2 <= 2 * r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a *2 <= 2 * r_a;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b *2 <= 2 * r_b;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_c *2 <= 2 * r_c;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_d *2 <= 2 * r_d;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
DROP TABLE iF EXISTS table_probe;
CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e BOOLEAN);
INSERT INTO table_probe VALUES 
 (null,6,16.532824,12.488346,cast(1 as BOOLEAN)), 
 (15,null,null,null,cast(0 as BOOLEAN)), 
 (13,4,null,4.984465,null), 
 (null,1,null,null,cast(0 as BOOLEAN)), 
 (14,null,null,null,null), 
 (null,null,null,16.616379,cast(0 as BOOLEAN)), 
 (null,2,19.902433,null,null), 
 (16,null,null,null,null), 
 (9,3,null,null,null), 
 (null,null,null,17.964407,null), 
 (8,2,null,17.849874,null), 
 (null,null,3.462826,11.477757,cast(0 as BOOLEAN)), 
 (null,19,null,17.064283,null), 
 (11,null,18.426025,null,cast(0 as BOOLEAN)), 
 (1,null,null,15.081567,null), 
 (5,null,7.874540,null,null), 
 (2,null,null,15.635139,null), 
 (null,null,null,15.498604,cast(1 as BOOLEAN)), 
 (4,16,null,null,cast(0 as BOOLEAN)), 
 (null,11,7.136741,1.558797,cast(0 as BOOLEAN)), 
 (14,17,null,null,null), 
 (null,20,null,null,cast(1 as BOOLEAN)), 
 (5,null,10.455925,3.098465,null), 
 (null,14,14.184505,null,null), 
 (null,null,2.804857,16.258356,cast(0 as BOOLEAN)), 
 (null,0,null,13.432361,null), 
 (10,null,null,2.537997,null), 
 (9,null,14.083692,11.856670,null), 
 (null,null,3.250931,null,cast(0 as BOOLEAN)), 
 (6,null,null,17.435667,null), 
 (null,4,null,null,cast(0 as BOOLEAN)), 
 (2,1,19.049723,4.815287,cast(0 as BOOLEAN)), 
 (5,null,null,null,cast(1 as BOOLEAN)), 
 (null,null,0.518320,null,cast(0 as BOOLEAN)), 
 (1,null,null,null,null), 
 (null,null,15.302996,16.828457,cast(1 as BOOLEAN)), 
 (4,8,null,null,null), 
 (null,5,null,null,null), 
 (17,4,5.107011,9.131957,cast(0 as BOOLEAN)), 
 (6,null,1.410239,null,cast(0 as BOOLEAN)), 
 (19,null,null,6.419842,null), 
 (17,null,null,null,cast(0 as BOOLEAN)), 
 (16,10,null,null,cast(0 as BOOLEAN)), 
 (null,14,null,14.416563,cast(0 as BOOLEAN)), 
 (13,9,null,null,cast(1 as BOOLEAN)), 
 (null,null,12.653252,null,null), 
 (null,null,null,null,null), 
 (5,null,null,10.107287,null), 
 (null,null,null,7.584749,null), 
 (19,null,null,null,cast(0 as BOOLEAN)), 
 (1,null,10.981024,12.948017,cast(1 as BOOLEAN)), 
 (15,13,8.684073,null,cast(0 as BOOLEAN)), 
 (7,null,6.354829,14.930886,null), 
 (null,null,null,4.392916,cast(1 as BOOLEAN)), 
 (null,19,7.939794,12.872362,null), 
 (8,null,null,null,cast(0 as BOOLEAN)), 
 (null,null,null,13.806643,cast(0 as BOOLEAN)), 
 (null,null,null,null,null), 
 (4,null,12.239152,null,null), 
 (0,18,null,null,null), 
 (null,null,null,null,null), 
 (1,null,12.028555,7.579372,cast(0 as BOOLEAN)), 
 (2,null,4.957312,15.291920,null), 
 (10,null,17.784986,null,null), 
 (null,20,11.765490,null,cast(0 as BOOLEAN)), 
 (null,null,0.590387,null,null), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (null,null,12.407209,8.822236,null), 
 (null,null,8.948468,null,cast(0 as BOOLEAN)), 
 (1,null,14.641761,8.941919,null), 
 (2,18,null,16.642015,cast(0 as BOOLEAN)), 
 (null,14,null,2.498627,cast(1 as BOOLEAN)), 
 (null,null,7.760050,null,null), 
 (7,20,null,1.024114,null), 
 (null,1,10.920438,null,cast(1 as BOOLEAN)), 
 (2,15,12.689019,19.918255,cast(0 as BOOLEAN)), 
 (null,null,null,5.484109,cast(0 as BOOLEAN)), 
 (20,0,null,null,null), 
 (0,null,14.567575,9.409244,cast(0 as BOOLEAN)), 
 (null,1,null,8.196544,null), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (1,null,null,14.100839,null), 
 (19,null,null,3.282244,null), 
 (19,11,10.205985,8.202678,null), 
 (6,8,13.662874,null,cast(0 as BOOLEAN)), 
 (null,null,0.814346,15.218477,cast(1 as BOOLEAN)), 
 (null,19,11.427382,null,null), 
 (null,14,null,null,null), 
 (null,1,13.400120,null,cast(1 as BOOLEAN)), 
 (null,null,4.664845,null,null), 
 (null,null,1.454633,null,null), 
 (17,11,0.545251,null,null), 
 (null,12,15.864285,11.945875,null), 
 (null,null,14.638290,12.668837,cast(0 as BOOLEAN)), 
 (null,null,null,null,null), 
 (null,null,6.801058,null,cast(0 as BOOLEAN)), 
 (20,13,16.755869,null,cast(0 as BOOLEAN)), 
 (15,13,null,null,null), 
 (2,null,16.739408,0.452969,null), 
 (null,20,null,null,null) 
;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b < r_b AND l_c > r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a < r_a AND l_d > r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a < r_a AND l_d > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b < r_b AND r_c > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b <= r_b AND l_c >= r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a <= r_a AND l_d >= r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a <= r_a AND l_d >= 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b <= r_b AND r_c >= 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b <= r_b AND l_c <> r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a <= r_a AND l_d <> r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a <= r_a AND l_d <> 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b <= r_b AND r_c <> 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b <> r_b AND l_c >= r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a <> r_a AND l_d >= r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a <> r_a AND l_d >= 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b <> r_b AND r_c >= 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
DROP TABLE iF EXISTS table_probe;
CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e BOOLEAN);
INSERT INTO table_probe VALUES 
 (5,16,null,null,cast(1 as BOOLEAN)), 
 (10,8,18.916866,null,cast(0 as BOOLEAN)), 
 (6,null,null,null,null), 
 (null,null,15.100751,null,cast(1 as BOOLEAN)), 
 (null,10,4.523460,19.911020,cast(1 as BOOLEAN)), 
 (2,null,19.831024,null,cast(1 as BOOLEAN)), 
 (12,15,0.489114,16.993715,null), 
 (null,1,null,1.782923,null), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (null,null,null,8.190707,cast(1 as BOOLEAN)), 
 (10,7,1.444221,null,null), 
 (17,null,null,0.997838,null), 
 (3,6,6.720059,null,cast(0 as BOOLEAN)), 
 (null,null,6.226213,19.382236,null), 
 (null,12,12.932501,2.098275,null), 
 (1,9,1.705893,null,cast(1 as BOOLEAN)), 
 (19,15,14.672795,16.665838,null), 
 (11,6,null,16.253355,null), 
 (0,null,0.608726,null,cast(1 as BOOLEAN)), 
 (null,null,null,2.259635,null), 
 (null,2,null,null,cast(0 as BOOLEAN)), 
 (15,null,null,null,cast(1 as BOOLEAN)), 
 (3,null,17.947615,13.519493,cast(1 as BOOLEAN)), 
 (null,8,null,null,cast(0 as BOOLEAN)), 
 (8,null,null,null,null), 
 (17,6,null,18.704563,cast(0 as BOOLEAN)), 
 (1,null,null,4.697450,cast(0 as BOOLEAN)), 
 (12,null,19.890354,null,cast(1 as BOOLEAN)), 
 (1,3,11.744864,null,null), 
 (null,null,null,null,cast(1 as BOOLEAN)), 
 (9,null,10.072426,null,cast(0 as BOOLEAN)), 
 (15,3,null,null,null), 
 (15,5,17.467445,null,null), 
 (null,15,11.918917,null,cast(0 as BOOLEAN)), 
 (15,null,14.832203,null,null), 
 (14,null,2.190122,12.416533,null), 
 (7,15,2.933640,12.621977,cast(0 as BOOLEAN)), 
 (null,null,null,4.812128,null), 
 (null,5,null,null,null), 
 (20,18,9.038834,6.298084,null), 
 (null,null,3.942274,15.511200,cast(1 as BOOLEAN)), 
 (null,10,null,16.307240,cast(0 as BOOLEAN)), 
 (9,9,2.769905,1.219180,null), 
 (null,15,16.844790,null,null), 
 (14,13,1.646940,17.688156,null), 
 (2,null,6.484112,null,null), 
 (17,null,null,null,null), 
 (8,12,12.611830,null,null), 
 (null,16,null,null,cast(0 as BOOLEAN)), 
 (3,6,12.880186,null,cast(1 as BOOLEAN)), 
 (null,15,4.275082,null,cast(1 as BOOLEAN)), 
 (8,11,null,9.836843,cast(1 as BOOLEAN)), 
 (12,null,null,10.510017,null), 
 (14,8,null,null,null), 
 (null,null,null,11.052078,cast(1 as BOOLEAN)), 
 (null,17,null,null,cast(0 as BOOLEAN)), 
 (null,3,null,null,null), 
 (null,10,null,1.979857,cast(1 as BOOLEAN)), 
 (19,3,6.061519,null,cast(1 as BOOLEAN)), 
 (null,null,null,18.675001,null), 
 (null,null,15.039679,null,cast(1 as BOOLEAN)), 
 (null,null,null,null,null), 
 (12,null,1.757318,null,cast(1 as BOOLEAN)), 
 (20,4,null,null,null), 
 (3,16,12.366879,2.268701,null), 
 (null,null,6.454769,4.211504,cast(1 as BOOLEAN)), 
 (null,5,null,null,cast(1 as BOOLEAN)), 
 (1,null,15.878527,null,null), 
 (7,null,0.376275,null,cast(0 as BOOLEAN)), 
 (null,14,null,15.438422,cast(1 as BOOLEAN)), 
 (10,null,5.377079,null,null), 
 (11,13,null,0.571361,cast(0 as BOOLEAN)), 
 (null,11,13.837408,null,cast(0 as BOOLEAN)), 
 (6,null,null,null,cast(1 as BOOLEAN)), 
 (null,14,17.243507,null,null), 
 (6,15,null,1.152828,cast(1 as BOOLEAN)), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (null,15,12.602511,3.337113,null), 
 (2,null,null,0.435971,cast(1 as BOOLEAN)), 
 (null,null,10.238560,null,null), 
 (8,null,13.605585,null,cast(1 as BOOLEAN)), 
 (null,6,null,null,cast(0 as BOOLEAN)), 
 (15,null,19.460981,19.142052,null), 
 (null,null,5.564721,null,cast(0 as BOOLEAN)), 
 (2,null,1.210849,12.798648,null), 
 (null,3,null,11.926550,null), 
 (null,null,12.900509,6.699518,null), 
 (null,20,14.547148,null,cast(0 as BOOLEAN)), 
 (6,null,5.911750,11.561426,cast(0 as BOOLEAN)), 
 (null,null,9.810566,2.327180,null), 
 (null,1,17.320955,14.184565,null), 
 (11,null,1.752335,null,null), 
 (null,1,null,13.853308,null), 
 (null,null,null,null,null), 
 (8,null,9.118292,null,null), 
 (9,null,null,null,null), 
 (1,null,4.987467,null,null), 
 (null,16,8.880291,null,null), 
 (3,null,6.081776,8.162846,cast(0 as BOOLEAN)), 
 (6,10,0.061271,null,cast(1 as BOOLEAN)) 
;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b < r_b OR l_c > r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a < r_a OR l_d > r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a < r_a OR l_d > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b < r_b OR r_c > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b > r_b OR l_c = r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a > r_a OR l_d = r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a > r_a OR l_d = 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b > r_b OR r_c = 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b = r_b OR l_c > r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a = r_a OR l_d > r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_a = r_a OR l_d > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe JOIN table_hash ON l_b = r_b OR r_c > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
DROP TABLE iF EXISTS table_probe;
CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e BOOLEAN);
INSERT INTO table_probe VALUES 
 (null,16,10.020657,11.906384,cast(1 as BOOLEAN)), 
 (8,12,12.742085,8.983748,cast(1 as BOOLEAN)), 
 (null,null,19.757019,null,cast(1 as BOOLEAN)), 
 (null,3,15.909727,17.968370,cast(1 as BOOLEAN)), 
 (null,12,null,null,cast(0 as BOOLEAN)), 
 (13,null,11.504913,18.125540,null), 
 (16,10,7.612779,9.520069,cast(1 as BOOLEAN)), 
 (14,null,3.543822,null,null), 
 (null,null,2.209780,17.778322,null), 
 (16,0,null,4.288202,cast(0 as BOOLEAN)), 
 (6,null,null,null,cast(1 as BOOLEAN)), 
 (5,null,null,null,cast(1 as BOOLEAN)), 
 (17,13,null,null,null), 
 (18,13,7.285090,5.547121,null), 
 (null,20,10.499377,1.723647,cast(0 as BOOLEAN)), 
 (8,null,null,7.562916,null), 
 (1,3,null,null,null), 
 (null,null,0.671623,11.055879,null), 
 (null,null,16.288059,18.586876,cast(0 as BOOLEAN)), 
 (4,6,null,null,null), 
 (null,null,7.243118,null,null), 
 (null,null,null,null,null), 
 (null,19,2.780598,null,cast(0 as BOOLEAN)), 
 (null,16,null,null,cast(1 as BOOLEAN)), 
 (14,null,5.069163,null,cast(0 as BOOLEAN)), 
 (9,null,8.881002,14.521140,null), 
 (null,null,12.026509,null,cast(0 as BOOLEAN)), 
 (14,null,null,3.115538,cast(1 as BOOLEAN)), 
 (null,16,null,null,cast(0 as BOOLEAN)), 
 (null,null,null,null,null), 
 (null,8,null,8.379740,null), 
 (null,8,null,null,null), 
 (5,null,3.639960,null,null), 
 (16,15,15.334421,null,null), 
 (null,null,8.630645,19.022406,null), 
 (null,14,9.338957,null,cast(1 as BOOLEAN)), 
 (null,11,6.916065,1.669473,cast(1 as BOOLEAN)), 
 (6,0,0.997761,14.724797,cast(0 as BOOLEAN)), 
 (null,null,9.810389,null,cast(0 as BOOLEAN)), 
 (null,null,0.718929,13.193359,cast(1 as BOOLEAN)), 
 (null,18,null,null,null), 
 (1,12,null,null,null), 
 (20,null,null,5.700007,cast(0 as BOOLEAN)), 
 (12,8,11.526253,5.499087,null), 
 (null,10,null,9.196141,cast(1 as BOOLEAN)), 
 (20,null,5.470943,12.987949,null), 
 (null,9,null,null,null), 
 (13,20,14.362607,7.530513,null), 
 (null,null,null,14.095303,cast(0 as BOOLEAN)), 
 (0,7,null,null,null), 
 (null,null,15.784989,10.995302,cast(1 as BOOLEAN)), 
 (20,null,null,3.958154,null), 
 (17,3,null,null,cast(0 as BOOLEAN)), 
 (null,null,12.191668,8.592943,cast(1 as BOOLEAN)), 
 (null,18,17.221239,null,null), 
 (11,null,null,null,null), 
 (13,null,null,null,cast(0 as BOOLEAN)), 
 (null,null,null,9.080156,cast(0 as BOOLEAN)), 
 (5,null,11.981802,12.274518,null), 
 (null,1,14.485958,null,cast(1 as BOOLEAN)), 
 (null,null,7.687588,null,cast(0 as BOOLEAN)), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (null,null,11.839615,4.464827,null), 
 (13,null,1.148471,0.367779,null), 
 (null,null,11.405413,null,null), 
 (11,5,19.022369,null,cast(0 as BOOLEAN)), 
 (15,6,15.090871,null,cast(0 as BOOLEAN)), 
 (10,null,null,null,null), 
 (15,null,null,null,cast(0 as BOOLEAN)), 
 (null,8,null,15.727512,null), 
 (3,10,null,4.051879,cast(0 as BOOLEAN)), 
 (12,null,8.389415,18.772465,cast(1 as BOOLEAN)), 
 (10,20,null,2.693851,cast(1 as BOOLEAN)), 
 (null,null,13.622437,9.184982,cast(0 as BOOLEAN)), 
 (7,17,null,5.409201,cast(1 as BOOLEAN)), 
 (20,2,18.701056,17.942833,null), 
 (null,null,4.621659,6.586982,null), 
 (null,0,17.089340,null,null), 
 (3,null,5.659513,0.478288,null), 
 (0,11,null,19.346237,null), 
 (null,15,2.440440,null,null), 
 (null,5,7.677165,null,cast(1 as BOOLEAN)), 
 (2,17,18.917627,null,cast(1 as BOOLEAN)), 
 (5,7,0.548792,8.300976,null), 
 (13,17,null,5.611484,null), 
 (null,null,9.014892,null,cast(0 as BOOLEAN)), 
 (6,5,null,13.295484,cast(1 as BOOLEAN)), 
 (20,null,0.861515,null,null), 
 (null,null,19.168385,null,cast(1 as BOOLEAN)), 
 (null,null,null,null,cast(1 as BOOLEAN)), 
 (null,null,10.611180,null,cast(0 as BOOLEAN)), 
 (5,null,14.358711,9.475052,cast(0 as BOOLEAN)), 
 (13,4,null,10.555664,null), 
 (1,null,15.398590,null,cast(0 as BOOLEAN)), 
 (9,17,7.260734,17.182463,null), 
 (null,10,null,8.819393,cast(0 as BOOLEAN)), 
 (5,14,10.455403,null,null), 
 (null,null,null,null,null), 
 (null,17,9.899482,null,cast(0 as BOOLEAN)), 
 (null,18,null,15.325641,cast(0 as BOOLEAN)) 
;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe JOIN table_hash ON l_b < r_b AND l_c > r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe JOIN table_hash ON l_a < r_a AND l_d > r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe JOIN table_hash ON l_a < r_a AND l_d > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT sum(l_a) from table_probe JOIN table_hash ON l_b < r_b AND r_c > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(l_b) from table_probe JOIN table_hash ON l_b = r_b OR l_c >= r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(l_b) from table_probe JOIN table_hash ON l_a = r_a OR l_d >= r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(l_b) from table_probe JOIN table_hash ON l_a = r_a OR l_d >= 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT max(l_b) from table_probe JOIN table_hash ON l_b = r_b OR r_c >= 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_c) from table_probe JOIN table_hash ON l_b <= r_b OR l_c = r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_c) from table_probe JOIN table_hash ON l_a <= r_a OR l_d = r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_c) from table_probe JOIN table_hash ON l_a <= r_a OR l_d = 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT min(l_c) from table_probe JOIN table_hash ON l_b <= r_b OR r_c = 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT NOT NULL, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
DROP TABLE iF EXISTS table_probe;
CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e BOOLEAN);
INSERT INTO table_probe VALUES 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (null,null,1.000000,null,null), 
 (null,2,2.000000,null,null), 
 (3,null,null,3.000000,cast(1 as BOOLEAN)), 
 (null,null,null,null,cast(0 as BOOLEAN)), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN)), 
 (6,6,null,6.000000,cast(0 as BOOLEAN)), 
 (null,7,null,null,null), 
 (8,null,null,null,cast(0 as BOOLEAN)), 
 (9,null,9.000000,null,null) 
;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b < r_b AND l_c > r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a < r_a AND l_d > r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a < r_a AND l_d > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b < r_b AND r_c > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b <= r_b AND l_c >= r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a <= r_a AND l_d >= r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a <= r_a AND l_d >= 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b <= r_b AND r_c >= 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b <= r_b AND l_c <> r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a <= r_a AND l_d <> r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a <= r_a AND l_d <> 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b <= r_b AND r_c <> 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b <> r_b AND l_c >= r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a <> r_a AND l_d >= r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a <> r_a AND l_d >= 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b <> r_b AND r_c >= 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b = r_b AND l_c > r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,null,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a = r_a AND l_d > r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,9.000000,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a = r_a AND l_d > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (null,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (null,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,null,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b = r_b AND r_c > 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (null,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,null,14.000000,cast(0 as BOOLEAN),14), 
 (null,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b > r_b AND l_c = r_c ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,null,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,null,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a > r_a AND l_d = r_d ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (null,4,null,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,null,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (8,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (null,12,12.000000,12.000000,cast(0 as BOOLEAN),12), 
 (null,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,null,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,18.000000,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_a > r_a AND l_d = 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (null,0,null,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,1.000000,1.000000,cast(1 as BOOLEAN),1), 
 (null,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (3,3,null,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (5,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (6,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (null,7,null,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (null,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (null,10,null,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,null,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,13.000000,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,15.000000,15.000000,cast(1 as BOOLEAN),15), 
 (16,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (17,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (null,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (null,19,null,19.000000,cast(1 as BOOLEAN),19) 
;
SELECT * from table_probe LEFT JOIN table_hash ON l_b > r_b AND r_c = 10 ;
DROP TABLE iF EXISTS table_hash;
CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e BOOLEAN, r_f INTEGER);
INSERT INTO table_hash VALUES 
 (0,0,0.000000,0.000000,cast(0 as BOOLEAN),0), 
 (1,1,null,1.000000,cast(1 as BOOLEAN),1), 
 (2,2,2.000000,2.000000,cast(0 as BOOLEAN),2), 
 (null,3,3.000000,3.000000,cast(1 as BOOLEAN),3), 
 (4,4,4.000000,4.000000,cast(0 as BOOLEAN),4), 
 (null,5,5.000000,5.000000,cast(1 as BOOLEAN),5), 
 (null,6,6.000000,6.000000,cast(0 as BOOLEAN),6), 
 (7,7,7.000000,7.000000,cast(1 as BOOLEAN),7), 
 (null,8,8.000000,8.000000,cast(0 as BOOLEAN),8), 
 (9,9,null,9.000000,cast(1 as BOOLEAN),9), 
 (10,10,10.000000,10.000000,cast(0 as BOOLEAN),10), 
 (11,11,11.000000,11.000000,cast(1 as BOOLEAN),11), 
 (12,12,null,12.000000,cast(0 as BOOLEAN),12), 
 (13,13,null,13.000000,cast(1 as BOOLEAN),13), 
 (14,14,14.000000,14.000000,cast(0 as BOOLEAN),14), 
 (15,15,null,15.000000,cast(1 as BOOLEAN),15), 
 (null,16,16.000000,16.000000,cast(0 as BOOLEAN),16), 
 (null,17,17.000000,17.000000,cast(1 as BOOLEAN),17), 
 (18,18,null,18.000000,cast(0 as BOOLEAN),18), 
 (19,19,19.000000,19.000000,cast(1 as BOOLEAN),19) 
;
