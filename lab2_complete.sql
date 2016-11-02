Lab 1:
Karolina Ziomek (karzi360) and Rasmus Holm (rasho258)

#### 1
Description: List all employees.

SELECT *
FROM jbemployee;

+------+--------------------+--------+---------+-----------+-----------+
| id   | name               | salary | manager | birthyear | startyear |
+------+--------------------+--------+---------+-----------+-----------+
|   10 | Ross, Stanley      |  15908 |     199 |      1927 |      1945 |
|   11 | Ross, Stuart       |  12067 |    NULL |      1931 |      1932 |
|   13 | Edwards, Peter     |   9000 |     199 |      1928 |      1958 |
|   26 | Thompson, Bob      |  13000 |     199 |      1930 |      1970 |
|   32 | Smythe, Carol      |   9050 |     199 |      1929 |      1967 |
|   33 | Hayes, Evelyn      |  10100 |     199 |      1931 |      1963 |
|   35 | Evans, Michael     |   5000 |      32 |      1952 |      1974 |
|   37 | Raveen, Lemont     |  11985 |      26 |      1950 |      1974 |
|   55 | James, Mary        |  12000 |     199 |      1920 |      1969 |
|   98 | Williams, Judy     |   9000 |     199 |      1935 |      1969 |
|  129 | Thomas, Tom        |  10000 |     199 |      1941 |      1962 |
|  157 | Jones, Tim         |  12000 |     199 |      1940 |      1960 |
|  199 | Bullock, J.D.      |  27000 |    NULL |      1920 |      1920 |
|  215 | Collins, Joanne    |   7000 |      10 |      1950 |      1971 |
|  430 | Brunet, Paul C.    |  17674 |     129 |      1938 |      1959 |
|  843 | Schmidt, Herman    |  11204 |      26 |      1936 |      1956 |
|  994 | Iwano, Masahiro    |  15641 |     129 |      1944 |      1970 |
| 1110 | Smith, Paul        |   6000 |      33 |      1952 |      1973 |
| 1330 | Onstad, Richard    |   8779 |      13 |      1952 |      1971 |
| 1523 | Zugnoni, Arthur A. |  19868 |     129 |      1928 |      1949 |
| 1639 | Choy, Wanda        |  11160 |      55 |      1947 |      1970 |
| 2398 | Wallace, Maggie J. |   7880 |      26 |      1940 |      1959 |
| 4901 | Bailey, Chas M.    |   8377 |      32 |      1956 |      1975 |
| 5119 | Bono, Sonny        |  13621 |      55 |      1939 |      1963 |
| 5219 | Schwarz, Jason B.  |  13374 |      33 |      1944 |      1959 |
+------+--------------------+--------+---------+-----------+-----------+


#### 2
Description: List all department names in alphabetical order.

SELECT name
FROM jbdept
ORDER BY name ASC;

+------------------+
| name             |
+------------------+
| Bargain          |
| Book             |
| Candy            |
| Children's       |
| Children's       |
| Furniture        |
| Giftwrap         |
| Jewelry          |
| Junior Miss      |
| Junior's         |
| Linens           |
| Major Appliances |
| Men's            |
| Sportswear       |
| Stationary       |
| Toys             |
| Women's          |
| Women's          |
| Women's          |
+------------------+


#### 3
Description: List the name of the parts that are not in store.

SELECT name
FROM jbparts
WHERE qoh = 0;

+-------------------+
| name              |
+-------------------+
| card reader       |
| card punch        |
| paper tape reader |
| paper tape punch  |
+-------------------+

#### 4
Description: List the names of employees with salary in [9000, 10000].

SELECT name
FROM jbemployee
WHERE salary>= 9000 AND salary <=10000;

+----------------+
| name           |
+----------------+
| Edwards, Peter |
| Smythe, Carol  |
| Williams, Judy |
| Thomas, Tom    |
+----------------+

#### 5
Description: List the name and the age when employees joined the company.

SELECT name, (startyear - birthyear) AS "age"
FROM jbemployee;

+--------------------+------+
| name               | age  |
+--------------------+------+
| Ross, Stanley      |   18 |
| Ross, Stuart       |    1 |
| Edwards, Peter     |   30 |
| Thompson, Bob      |   40 |
| Smythe, Carol      |   38 |
| Hayes, Evelyn      |   32 |
| Evans, Michael     |   22 |
| Raveen, Lemont     |   24 |
| James, Mary        |   49 |
| Williams, Judy     |   34 |
| Thomas, Tom        |   21 |
| Jones, Tim         |   20 |
| Bullock, J.D.      |    0 |
| Collins, Joanne    |   21 |
| Brunet, Paul C.    |   21 |
| Schmidt, Herman    |   20 |
| Iwano, Masahiro    |   26 |
| Smith, Paul        |   21 |
| Onstad, Richard    |   19 |
| Zugnoni, Arthur A. |   21 |
| Choy, Wanda        |   23 |
| Wallace, Maggie J. |   19 |
| Bailey, Chas M.    |   19 |
| Bono, Sonny        |   24 |
| Schwarz, Jason B.  |   15 |
+--------------------+------+

#### 6
Desciption: List the names of the employees with last name ending in "son".

SELECT name
FROM jbemployee
WHERE name like "%son,%";

+---------------+
| name          |
+---------------+
| Thompson, Bob |
+---------------+

#### 7
Description: List the item names that are supplied by Fisher-Price.

SELECT name
FROM jbitem item
WHERE exists (SELECT *
              FROM jbsupplier s
              WHERE s.name="Fisher-Price" AND s.id=item.supplier);

+-----------------+
| name            |
+-----------------+
| Maze            |
| The 'Feel' Book |
| Squeeze Ball    |
+-----------------+

#### 8
Description: List the item names that are supplied by Fisher-Price.

SELECT jbitem.name
FROM jbitem, jbsupplier
WHERE jbitem.supplier = jbsupplier.id AND jbsupplier.name="Fisher-Price";

+-----------------+
| name            |
+-----------------+
| Maze            |
| The 'Feel' Book |
| Squeeze Ball    |
+-----------------+

#### 9
Description: List the name of the cities with suppliers.

SELECT name
FROM jbcity city
WHERE exists (SELECT *
              FROM jbsupplier supplier
              WHERE city.id = supplier.city);

+----------------+
| name           |
+----------------+
| Amherst        |
| Boston         |
| New York       |
| White Plains   |
| Hickville      |
| Atlanta        |
| Madison        |
| Paxton         |
| Dallas         |
| Denver         |
| Salt Lake City |
| Los Angeles    |
| San Diego      |
| San Francisco  |
| Seattle        |
+----------------+


#### 10
Description: List the name and color of items that weight more than the card reader.

SELECT name, color
FROM jbparts
WHERE weight > (SELECT weight
                FROM jbparts
                WHERE name="card reader");

+--------------+--------+
| name         | color  |
+--------------+--------+
| disk drive   | black  |
| tape drive   | black  |
| line printer | yellow |
| card punch   | gray   |
+--------------+--------+

##### 11
Description: List the name and color of items that weight more than the card reader.

SELECT part1.name, part1.color
FROM jbparts part1, jbparts part2
WHERE part1.weight > part2.weight AND part2.name="card reader";

+--------------+--------+
| name         | color  |
+--------------+--------+
| disk drive   | black  |
| tape drive   | black  |
| line printer | yellow |
| card punch   | gray   |
+--------------+--------+

##### 12
Description: The average weight of black parts.

SELECT AVG(weight) AS "average weight"
FROM jbparts
WHERE color = "black";

+----------------+
| average weight |
+----------------+
|       347.2500 |
+----------------+

##### 13
Desciption: List the total weights delivered by the suppliers in Massachusetts.

SELECT supplier.name, SUM(supply.quan * parts.weight) AS "total weight"
FROM jbsupplier supplier
INNER JOIN
(jbcity city, jbsupply supply, jbparts parts)
ON (supplier.city=city.id AND city.state="MASS" AND supply.supplier = supplier.id AND supply.part = parts.id)
GROUP BY supplier.name;

+--------------+--------------+
| name         | total weight |
+--------------+--------------+
| DEC          |         3120 |
| Fisher-Price |      1135000 |
+--------------+--------------+

##### 14
Description: Creates a new table and fills it with items that cost less than the average price.

CREATE TABLE  item_ltap (
       id INT  NOT NULL,
       name CHAR (32) NOT NULL,
       dept INT NOT NULL,
       price INT NOT NULL,
       qoh INT NOT NULL,
       supplier INT NOT NULL,
       PRIMARY KEY (id),
       FOREIGN KEY (id) REFERENCES jbitem (id),
       FOREIGN KEY (supplier) REFERENCES jbsupplier (id),
       FOREIGN KEY (dept) REFERENCES jbdept (id)
) AS (SELECT *
      FROM jbitem
      WHERE price < (SELECT AVG(price)
                     FROM jbitem));

+-----+-----------------+------+-------+------+----------+
| id  | name            | dept | price | qoh  | supplier |
+-----+-----------------+------+-------+------+----------+
|  11 | Wash Cloth      |    1 |    75 |  575 |      213 |
|  19 | Bellbottoms     |   43 |   450 |  600 |       33 |
|  21 | ABC Blocks      |    1 |   198 |  405 |      125 |
|  23 | 1 lb Box        |   10 |   215 |  100 |       42 |
|  25 | 2 lb Box, Mix   |   10 |   450 |   75 |       42 |
|  26 | Earrings        |   14 |  1000 |   20 |      199 |
|  43 | Maze            |   49 |   325 |  200 |       89 |
| 106 | Clock Book      |   49 |   198 |  150 |      125 |
| 107 | The 'Feel' Book |   35 |   225 |  225 |       89 |
| 118 | Towels, Bath    |   26 |   250 | 1000 |      213 |
| 119 | Squeeze Ball    |   49 |   250 |  400 |       89 |
| 120 | Twin Sheet      |   26 |   800 |  750 |      213 |
| 165 | Jean            |   65 |   825 |  500 |       33 |
| 258 | Shirt           |   58 |   650 | 1200 |       33 |
+-----+-----------------+------+-------+------+----------+
