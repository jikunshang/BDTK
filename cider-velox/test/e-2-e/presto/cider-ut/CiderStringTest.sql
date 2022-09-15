DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (0,'0000000000'), 
 (1,'1111111111'), 
 (2,'2222222222'), 
 (3,'3333333333'), 
 (4,'4444444444'), 
 (5,'5555555555'), 
 (6,'6666666666'), 
 (7,'7777777777'), 
 (8,'8888888888'), 
 (9,'9999999999') 
;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (0,'0000000000'), 
 (1,'1111111111'), 
 (2,'2222222222'), 
 (3,'3333333333'), 
 (4,'4444444444'), 
 (5,'5555555555'), 
 (6,'6666666666'), 
 (7,'7777777777'), 
 (8,'8888888888'), 
 (9,'9999999999') 
;
SELECT SUBSTRING(col_2, 1, 10) FROM test ;
SELECT SUBSTRING(col_2, 1, 8) FROM test ;
SELECT SUBSTRING(col_2, 4, 8) FROM test ;
SELECT SUBSTRING(col_2, 0, 12) FROM test ;
SELECT SUBSTRING(col_2, 12, 0) FROM test ;
SELECT SUBSTRING(col_2, 12, 2) FROM test ;
SELECT SUBSTRING(col_2 from 2 for 8) FROM test ;
SELECT SUBSTRING(col_2, 4, 0) FROM test ;
SELECT SUBSTRING(col_2, -4, 2) FROM test ;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (0,'0000000000'), 
 (1,'1111111111'), 
 (2,'2222222222'), 
 (3,'3333333333'), 
 (4,'4444444444'), 
 (5,'5555555555'), 
 (6,'6666666666'), 
 (7,'7777777777'), 
 (8,'8888888888'), 
 (9,'9999999999') 
;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (0,'0000000000'), 
 (1,'1111111111'), 
 (2,'2222222222'), 
 (3,'3333333333'), 
 (4,'4444444444'), 
 (5,'5555555555'), 
 (6,'6666666666'), 
 (7,'7777777777'), 
 (8,'8888888888'), 
 (9,'9999999999') 
;
SELECT col_2 FROM test ;
SELECT col_1, col_2 FROM test ;
SELECT * FROM test ;
SELECT col_2 FROM test where col_2 = 'aaaa';
SELECT col_2 FROM test where col_2 = '0000000000';
SELECT col_2 FROM test where col_2 <> '0000000000';
SELECT col_1 FROM test where col_2 <> '1111111111';
SELECT col_1, col_2 FROM test where col_2 <> '2222222222';
SELECT * FROM test where col_2 <> 'aaaaaaaaaaa';
SELECT * FROM test where col_2 <> 'abcdefghijklmn';
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (0,'0000000000'), 
 (1,'1111111111'), 
 (2,'2222222222'), 
 (3,'3333333333'), 
 (4,'4444444444'), 
 (5,'5555555555'), 
 (6,'6666666666'), 
 (7,'7777777777'), 
 (8,'8888888888'), 
 (9,'9999999999') 
;
SELECT col_2 FROM test where col_2 LIKE '%1111';
SELECT col_2 FROM test where col_2 LIKE '1111%';
SELECT col_2 FROM test where col_2 LIKE '%1111%';
SELECT col_2 FROM test where col_2 LIKE '%1234%';
SELECT col_2 FROM test where col_2 LIKE '22%22';
SELECT col_2 FROM test where col_2 LIKE '_33%';
SELECT col_2 FROM test where col_2 LIKE '44_%';
SELECT col_2 FROM test where col_2 LIKE '5555%' OR col_2 LIKE '%6666';
SELECT col_2 FROM test where col_2 LIKE '7777%' AND col_2 LIKE '%8888';
SELECT col_2 FROM test where col_2 LIKE '%1111';
SELECT col_2 FROM test where col_2 NOT LIKE '1111%';
SELECT col_2 FROM test where col_2 NOT LIKE '44_4444444';
SELECT col_2 FROM test where col_2 NOT LIKE '44_4%' and col_2 NOT LIKE '%111%';
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (0,'0000000000'), 
 (1,'1111111111'), 
 (2,'2222222222'), 
 (3,'3333333333'), 
 (4,'4444444444'), 
 (5,'5555555555'), 
 (6,'6666666666'), 
 (7,'7777777777'), 
 (8,'8888888888'), 
 (9,'9999999999') 
;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (18,'a37J'), 
 (14,'cCHryDsbza'), 
 (8,'y4cBWDxS2'), 
 (2,'JjzhMaiR'), 
 (17,'V41mtzx'), 
 (7,'YvK'), 
 (4,'rO72tK0LK0e'), 
 (19,'zLOZ2nOXpPIhMFSv8k'), 
 (19,'07'), 
 (3,'20o0J90xA0GWXII') 
;
SELECT col_2 FROM test ;
SELECT col_1, col_2 FROM test ;
SELECT * FROM test ;
SELECT col_2 FROM test where col_2 = 'aaaa';
SELECT col_2 FROM test where col_2 = '0000000000';
SELECT col_2 FROM test where col_2 <> '0000000000';
SELECT col_1 FROM test where col_2 <> '1111111111';
SELECT col_1, col_2 FROM test where col_2 <> '2222222222';
SELECT * FROM test where col_2 <> 'aaaaaaaaaaa';
SELECT * FROM test where col_2 <> 'abcdefghijklmn';
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (11,'o7J4ogH'), 
 (11,'ZQxwQ2RQ0DRJKRE'), 
 (3,'PVzxlFrXL8b7m'), 
 (18,'KLHIGh'), 
 (18,'h5JuWcFwrgJKdE3t5'), 
 (5,'ECALy3eKIwYxEF3V7Z8'), 
 (2,'Tx0nFe1IX5t'), 
 (10,'H22'), 
 (9,'5gXOa5LnIM'), 
 (15,'QuOiNJj') 
;
SELECT col_2 FROM test where col_2 LIKE '%1111';
SELECT col_2 FROM test where col_2 LIKE '1111%';
SELECT col_2 FROM test where col_2 LIKE '%1111%';
SELECT col_2 FROM test where col_2 LIKE '%1234%';
SELECT col_2 FROM test where col_2 LIKE '22%22';
SELECT col_2 FROM test where col_2 LIKE '_33%';
SELECT col_2 FROM test where col_2 LIKE '44_%';
SELECT col_2 FROM test where col_2 LIKE '5555%' OR col_2 LIKE '%6666';
SELECT col_2 FROM test where col_2 LIKE '7777%' AND col_2 LIKE '%8888';
SELECT col_2 FROM test where col_2 LIKE '%1111';
SELECT col_2 FROM test where col_2 NOT LIKE '1111%';
SELECT col_2 FROM test where col_2 NOT LIKE '44_4444444';
SELECT col_2 FROM test where col_2 NOT LIKE '44_4%' and col_2 NOT LIKE '%111%';
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (18,'YL8rqDiZSk'), 
 (4,'foEDA'), 
 (8,'GTX'), 
 (6,'qqvkCd5WKE2'), 
 (4,'MtVXa2zKae6op'), 
 (16,'Y4i'), 
 (2,'bYuUG67LaSX'), 
 (6,'5tUbO4bNPB0TxnkWrSaQ'), 
 (16,'UuEa'), 
 (17,'X9') 
;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (9,'5mVwG4JLgeip'), 
 (13,'BlQtFFJpgHJYTrWz0w2'), 
 (15,'Qw1UFK8u2yWBjw3yCMl'), 
 (19,'c4'), 
 (8,'3tt2un4cDzdiEvq8v'), 
 (7,'f7TZAPjUAZ6Cu86'), 
 (20,'AyYD'), 
 (18,'mCCSQ7GX33A8WhGwRk40'), 
 (15,'HuxNf5JEItyS3QrBgOCh'), 
 (7,'KCDa6eIAd7RV4m') 
;
SELECT SUBSTRING(col_2, 1, 10) FROM test ;
SELECT SUBSTRING(col_2, 1, 8) FROM test ;
SELECT SUBSTRING(col_2, 4, 8) FROM test ;
SELECT SUBSTRING(col_2 from 2 for 8) FROM test ;
SELECT SUBSTRING(col_2, 4, 0) FROM test ;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (19,''), 
 (6,'5NQ'), 
 (3,'Jt0j'), 
 (14,''), 
 (11,''), 
 (15,''), 
 (16,''), 
 (18,''), 
 (11,'cdFnDLSWV3'), 
 (18,''), 
 (10,''), 
 (17,'ghhol4EgN5e4po'), 
 (9,''), 
 (16,''), 
 (9,'VVlkJw5jSYm4T'), 
 (5,'i92Ws4iYQoCSbysV'), 
 (17,''), 
 (17,''), 
 (11,''), 
 (19,'5Fl8wC') 
;
SELECT col_2 FROM test ;
SELECT col_1, col_2 FROM test ;
SELECT * FROM test ;
SELECT col_2 FROM test where col_2 = 'aaaa';
SELECT col_2 FROM test where col_2 = '0000000000';
SELECT col_2 FROM test where col_2 <> '0000000000';
SELECT col_1 FROM test where col_2 <> '1111111111';
SELECT col_1, col_2 FROM test where col_2 <> '2222222222';
SELECT * FROM test where col_2 <> 'aaaaaaaaaaa';
SELECT * FROM test where col_2 <> 'abcdefghijklmn';
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (7,'iE81uF1O736dRsouSmm'), 
 (10,'q8tfB'), 
 (6,'PK3Zzmn5lhLm5Qn92F2'), 
 (3,''), 
 (16,'UatPR1G4D'), 
 (15,'RVR0SBlXwQqgTFRdHgd5'), 
 (13,'5ffS4gi9r6YKVZmgIIa'), 
 (5,''), 
 (3,''), 
 (10,''), 
 (2,''), 
 (9,'fncKQh5TLkvPPc'), 
 (11,''), 
 (13,'g5'), 
 (11,''), 
 (5,'eJpubNdiZq3CbeW2Jc'), 
 (3,''), 
 (16,'KP4j1ayff'), 
 (2,'qHqdCQ0n8Xb9jDn'), 
 (16,'F7oij85ls4MqjzL') 
;
SELECT col_2 FROM test where col_2 LIKE '%1111';
SELECT col_2 FROM test where col_2 LIKE '1111%';
SELECT col_2 FROM test where col_2 LIKE '%1111%';
SELECT col_2 FROM test where col_2 LIKE '%1234%';
SELECT col_2 FROM test where col_2 LIKE '22%22';
SELECT col_2 FROM test where col_2 LIKE '_33%';
SELECT col_2 FROM test where col_2 LIKE '44_%';
SELECT col_2 FROM test where col_2 LIKE '5555%' OR col_2 LIKE '%6666';
SELECT col_2 FROM test where col_2 LIKE '7777%' AND col_2 LIKE '%8888';
SELECT col_2 FROM test where col_2 LIKE '%1111';
SELECT col_2 FROM test where col_2 NOT LIKE '1111%';
SELECT col_2 FROM test where col_2 NOT LIKE '44_4444444';
SELECT col_2 FROM test where col_2 NOT LIKE '44_4%' and col_2 NOT LIKE '%111%';
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));
INSERT INTO test VALUES 
 (6,''), 
 (8,''), 
 (9,'APZ8Cffop'), 
 (18,'1adEfRuPX0AP2UDmSWHh'), 
 (10,''), 
 (11,''), 
 (14,'DaIrE4eb5EEJu'), 
 (13,''), 
 (18,'HACPYCulwMIE1wg5'), 
 (14,'ENyQSc1VpFnj'), 
 (8,'z019PZLHIIbYWaSA'), 
 (3,''), 
 (16,'M3WnT'), 
 (5,''), 
 (17,''), 
 (3,'w2jdsibrryODEhTpF'), 
 (2,''), 
 (15,''), 
 (19,'73GT6kGXr5Ul7DOxwx'), 
 (6,'') 
;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 VARCHAR(10));
INSERT INTO test VALUES 
 ('1111111'), 
 ('1112222'), 
 ('aaaaaaa'), 
 ('bbbbbbbb'), 
 ('aabbccdd') 
;
SELECT col_1 FROM test where col_1 LIKE '[aa]%';
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 VARCHAR(10), col_2 VARCHAR(10));
INSERT INTO test VALUES 
 ('aaaaaaa','1'), 
 ('aabbccdd','2'), 
 ('','3'), 
 ('aaaaaaa','4'), 
 ('ddd','5'), 
 ('aabbccdd','6'), 
 ('',''), 
 ('','') 
;
SELECT col_1, COUNT(*) FROM test GROUP BY col_1;
DROP TABLE iF EXISTS test;
CREATE TABLE test(col_1 VARCHAR(10), col_2 VARCHAR(10));
INSERT INTO test VALUES 
 ('aaaaaaa','1'), 
 ('aabbccdd','2'), 
 ('','3'), 
 ('aaaaaaa','4'), 
 ('ddd','5'), 
 ('aabbccdd','6'), 
 ('',''), 
 ('','') 
;
SELECT col_1, COUNT(*), col_2 FROM test GROUP BY col_1, col_2;
