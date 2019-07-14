DROP TABLE IF EXISTS farmer;
CREATE TABLE IF NOT EXISTS farmer (
  name varchar(32) NOT NULL,
  lastname varchar(32) NOT NULL,
  addr varchar(100) NOT NULL,
  zip int(5) NOT NULL,
  city varchar(32) NOT NULL,
  PRIMARY KEY (name,lastname,addr)
) DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS farmerPhone;
CREATE TABLE IF NOT EXISTS farmerPhone (
  name varchar(32) NOT NULL,
  lastname varchar(32) NOT NULL,
  addr varchar(100) NOT NULL,
  phoneNumber varchar(32) NOT NULL,
  PRIMARY KEY (phoneNumber)
) DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS farmerEmail;
CREATE TABLE IF NOT EXISTS farmerEmail (
  name varchar(32) NOT NULL,
  lastname varchar(32) NOT NULL,
  addr varchar(100) NOT NULL,
  email varchar(100) NOT NULL,
  PRIMARY KEY (email)
) DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS market;
CREATE TABLE IF NOT EXISTS market (
  name varchar(32) NOT NULL,
  addr varchar(32) NOT NULL,
  zip int(5) NOT NULL,
  city varchar(32) NOT NULL,
  phoneNumber varchar(32) NOT NULL,
  budget int(128) NOT NULL,
  PRIMARY KEY (phoneNumber)
) DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS product;
CREATE TABLE IF NOT EXISTS product (
  name varchar(32) NOT NULL,
  plant_date varchar(32) NOT NULL,
  harvest_date varchar(32) NOT NULL,
  altitude int(100) NOT NULL,
  min_temp int(32) NOT NULL,
  hardness TINYINT NOT NULL,
  CHECK (hardness>=1 AND hardness<=20),
  PRIMARY KEY (name)
) DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS buys;
CREATE TABLE IF NOT EXISTS buys (
  transactionNum int NOT NULL AUTO_INCREMENT, 
  fname varchar(32) NOT NULL,
  flastname varchar(32) NOT NULL,
  pname varchar(32) NOT NULL,
  mname varchar(32) NOT NULL,
  maddr varchar(32) NOT NULL,
  amount int(100) NOT NULL,
  creditCard varchar(32) NOT NULL,
  PRIMARY KEY (transactionNum)
) DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS produces;
CREATE TABLE IF NOT EXISTS produces (
  produceNum int NOT NULL AUTO_INCREMENT, 
  fname varchar(32) NOT NULL,
  flastname varchar(32) NOT NULL,
  pname varchar(32) NOT NULL,
  amount int(100) NOT NULL,
  year int(32) NOT NULL,
  PRIMARY KEY (produceNum)
) DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS registers;
CREATE TABLE IF NOT EXISTS registers (
  regNum int NOT NULL AUTO_INCREMENT, 
  fname varchar(32) NOT NULL,
  flastname varchar(32) NOT NULL,
  pname varchar(32) NOT NULL,
  amount int(100) NOT NULL,
  price float(32) NOT NULL,
  IBAN varchar(100) NOT NULL,
  PRIMARY KEY (regNum)
) DEFAULT CHARSET=utf8mb4;


/* Farmer Insertion TEST */
INSERT INTO farmer () VALUES ('Cevdet','Sututan','14 Cikmaz Sk',42050,'Konya');
INSERT INTO farmerPhone () VALUES ('Cevdet','Sututan','14 Cikmaz Sk','5352223444');
INSERT INTO farmerPhone () VALUES ('Cevdet','Sututan','14 Cikmaz Sk','5352223445');
INSERT INTO farmerEmail () VALUES ('Cevdet','Sututan','14 Cikmaz Sk','cevdet@sucu.com');
INSERT INTO farmerEmail () VALUES ('Cevdet','Sututan','14 Cikmaz Sk','cevdet2@sucu.com');

/* Market Insertion TEST */
INSERT INTO market () VALUES ('Bim','Ilginc sk',42050,'Konya','5352223444',100000);
INSERT INTO market () VALUES ('Bim','Ilginc sk',42050,'Konya','5352223445',100000);

/* Product Insertion TEST */
INSERT INTO product () VALUES ('Grain','February','June',4000,2,1);
INSERT INTO product () VALUES ('Soy bean','March','August',2000,12,3);

/******************/
/* Register Insertion TEST */
INSERT INTO registers (fname,flastname,pname,amount,price,IBAN) 
				VALUES ('Cevdet','Sututan','Grain',180000,1.11,'TR1234570000000010');
INSERT INTO registers (fname,flastname,pname,amount,price,IBAN) 
				VALUES ('Cevdet','Sututan','Soy bean',50000,2.00,'TR1234570000000050');

/* Produce Insertion TEST */
INSERT INTO produces (fname,flastname,pname,amount,year) VALUES ('Cevdet','Sututan','Grain',140000,2015);
INSERT INTO produces (fname,flastname,pname,amount,year) VALUES ('Cevdet','Sututan','Grain',120000,2016);

/* Buy Insertion TEST */
INSERT INTO buys (fname,flastname,pname,mname,maddr,amount,creditCard) 
				VALUES ('Cevdet','Sututan','Grain','Bim','Ilginc sk',18000,'1230000000000010');
INSERT INTO buys (fname,flastname,pname,mname,maddr,amount,creditCard) 
				VALUES ('Cevdet','Sututan','Soy bean','Migros','Zengin Mh',5000,'1230000000000050');



INSERT INTO registers (fname,flastname,pname,amount,price,IBAN) 
SELECT * FROM (SELECT 'Cevdet','Sututan','Grain',180000,1.11,'TR1234570000000010') AS tmp
WHERE EXISTS (
    SELECT P.name FROM product P, farmer F WHERE P.name = 'Grain' AND F.name = 'Cevdet' AND F.lastname = 'Sututan'
) LIMIT 1;









/* Q1 */ 
SELECT P.fname, P.flastname, P.pname
FROM (	SELECT pname, MAX(amount) AS maxAmount
		FROM produces GROUP BY pname) AS TMP, produces P
WHERE P.pname = TMP.pname AND P.amount = TMP.maxAmount;

/* Q2 */ 
SELECT B.pname, B.fname, B.flastname, B.amount
FROM (SELECT pname, MAX(amount) AS maxAmount
		FROM buys GROUP BY pname ) AS TMP, buys B
WHERE B.pname = TMP.pname AND B.amount = TMP.maxAmount;

/* Q3 */ 
SELECT R.fname, R.flastname, R.pname, (TMP.amount*R.price) as cash
FROM (SELECT fname, flastname, pname, MAX(amount) as amount FROM buys GROUP BY fname,flastname ) AS TMP , registers R
WHERE R.fname = TMP.fname AND R.flastname = TMP.flastname AND R.pname = TMP.pname
ORDER BY cash DESC
LIMIT 1;

SELECT FF.fname, FF.flastname
FROM (SELECT R.fname, R.flastname, R.pname, (TMP.amount*R.price) as cash
FROM (SELECT fname, flastname, pname, MAX(amount) as amount FROM buys GROUP BY fname,flastname ) AS TMP , registers R
WHERE R.fname = TMP.fname AND R.flastname = TMP.flastname AND R.pname = TMP.pname
ORDER BY cash DESC
LIMIT 1) AS FF;


/* Working semi-true Q3 solution */
SELECT R.fname, R.flastname, (TMP.amount*R.price) as rslt
FROM(	SELECT fname, flastname, pname, amount
		FROM buys 
		WHERE amount = (SELECT MAX(amount) FROM buys )) AS TMP, registers R
WHERE R.fname = TMP.fname AND R.flastname = TMP.flastname AND R.pname = TMP.pname;

/* Q4 */ 
SELECT M.city, M.name
FROM (	SELECT city, name, MAX(budget) AS maxBudget
		FROM market GROUP BY city ) AS TMP, market M
WHERE M.city = TMP.city AND M.budget = TMP.maxBudget;

/* Q5 */ 
SELECT F.numFarmer + M.numMarket as Total_Users
FROM (	SELECT COUNT(*) AS numFarmer
		FROM farmer
) AS F
CROSS JOIN(	SELECT COUNT(*) AS numMarket
			FROM market
			) as M;





