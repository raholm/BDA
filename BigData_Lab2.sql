#### 1
SELECT *
FROM jbemployee;

#### 2
SELECT name
FROM jbdept
ORDER BY name ASC;

#### 3
SELECT name
FROM jbparts
WHERE qoh = 0;

#### 4
SELECT name
FROM jbemployee
WHERE salary>= 9000 AND salary <=10000;

#### 5
SELECT name, (startyear - birthyear) as age
FROM jbemployee;

#### 6
SELECT name
FROM jbemployee
WHERE name like „%son,%“;


#### 7
SELECT name
FROM jbitem item
WHERE exists (SELECT *
                          FROM jbsupplier s
                          WHERE s.name=„Fisher-Price“ AND s.id=item.supplier);

#### 8
SELECT name
FROM jbitem, jbsupplier
WHERE jbitem.supplier=jbsupplier.id AND jbsupplier.name=„Fisher-Price“;

#### 9
SELECT name
FROM jbcity city
WHERE exists (SELECT *
                          FROM jbsupplier s
                          WHERE city.id=s.city);


#### 10    1. Version ging und die anderen auch, aber die 1. ist die beste, weil am wenigsten Schreib
Arbeit
SELECT name, color
FROM jbparts part1
WHERE weight > (SELECT weight
                              FROM jbparts
                               WHERE name=„card reader“);

SELCET name, color
FROM jbparts part1, jbparts part2
WHERE part1.weight>part2.weight AND part2.name=„card reader“;

SELECHT name, color
FROM jbparts part1
WHERE exists (SELECT *
                          FROM jbparts part2
                          WHERE part1.weight>part2.weight AND part2.name=„card reader“);


#####11  ohne subquery!!!
SELCET name, color
FROM jbparts part1, jbparts part2
WHERE part1.weight>part2.weight AND part2.name=„card reader“;

#####12
SELECT avg(weight) as „average weight“
FROM jbparts
WHERE color=„black“;


#####13
(SELECT supplier.name, supplier.id
FROM jbsupplier supplier, jbcity city
WHERE supplier.city = city.id AND city.state=„Mass“) as suppliers join
(SELECT supply.part, supply.quan
FROM jbsupplier supplier, jbsupply supply
WHERE supply.supplier = supplier.id) as supply on suppliers.id=supply.supplier

SELECT parts.weight
FROM jbparts parts, jbsupply supply
WHERE parts.id=supply.part;

SELECT name, sum(quant*weight) as „total weight“
FROM
((SELECT supplier.name, supplier.id
FROM jbsupplier supplier, jbcity city
WHERE supplier.city = city.id AND city.state=„Mass“) as suppliers
INNER JOIN
(SELECT supply.supplier, supply.part, supply.quan     ## supply.supplier, um damit die zweite Tabelle zu joinen#####
FROM jbsupplier supplier, jbsupply supply
WHERE supply.supplier = supplier.id) as supply
ON suppliers.id=supply.supplier))
INNER JOIN
(SELECT id, weight
FROM jbparts parts) as parts
ON parts.id=part
GROUP BY name;

#### schönere Lösung
SELECT si.name, SUM(s.quant *p.weight) „total weight“
FROM jbsupplier si
INNER JOIN
(jbcity c, jbsupply s, jbparts p) ON
(city=c.id AND c.state=„MASS“ AND s.upplier= si.id AND s.part = p.id)
GROUP BY si.name;


#####14 Idee: man erstellt erst eine neue Tabelle, dann füllt man sie aber nur mit ein paar Daten,
nämlich allen, die kleiner sind als der Durchschnittspreis, deshalb CREATE TABLE () und dann AS (
mit der eigentlichen Unterabfrage
nur Vorbereitung:
[SELECT *
FROM jbitem
WHERE price < (SELECT avg(price)
                           FROM jbitem);   ]



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

       )

AS (SELECT *
	FROM jbitem
	WHERE price < (SELECT avg(price)
                           	FROM jbitem));


#####15 View erstellen

CREATE VIEW item_ltap_view AS
SELECT *
	FROM jbitem
	WHERE price < (SELECT avg(price)
                           	FROM jbitem);

##### 16
View is virtuell, does not exist in reality….siehe DB Folienset 5
außerdem: View ist statisch, Table ist dynamic, in der view kann ich keine delete, add Sachen machen, nur aus der Sicht halt noch Daten reduzieren, sie aber nicht tatsächlich löschen

####17
implicit join notation = wahrscheinlich das mit dem Komma in der FROM-Zeile

CREATE VIEW debit AS
SELECT sale.debit as „sales identifier“, SUM(sale.quantity*item.price) as „Total cost“
FROM jbsale sale, jbitem item
WHERE sale.item = item.id
GROUP BY sale.debit;

#####18 on brauche ich trotzdem, das ist quasi dann der Inhalt meiner Wheee-Abfrage, muss ja
klar sein, bzgl. welcher Spalte die machen sollen

CREATE VIEW debit AS
SELECT sale.debit as „sales identifier“, SUM(sale.quantity*item.price) as „Total cost“
FROM jbsale sale LEFT JOIN jbitem item on sale.item=item.id
GROUP BY sale.debit;

######19
### braucht man angeblich nicht
DELETE FROM jbsupply
WHERE supplier = (SELECT id
			       FROM jbsupplier
			       WHERE id = (SELECT id
                       				      FROM jbcity
                       				      WHERE name = „Los Angeles“));

#### braucht man angeblich nicht


DELETE FROM jbsale
WHERE item IN (SELECT id
                             FROM jbitem
                             WHERE supplier IN (SELECT id
			                                      FROM jbsupplier
			                                      WHERE id = (SELECT id
                       				                                   FROM jbcity
                       				                                   WHERE name = „Los Angeles“));

DELETE FROM jbitem
WHERE supplier = (SELECT id
			       FROM jbsupplier
			       WHERE id = (SELECT id
                       				      FROM jbcity
                       				      WHERE name = „Los Angeles“));

DELETE FROM jbsupplier
WHERE id = (SELECT id
			       FROM jbsupplier
			       WHERE id = (SELECT id
                       				      FROM jbcity
                       				      WHERE name = „Los Angeles“));







Vorbereitung:
SELECT id
FROM jbsupplier
WHERE id = (SELECT id
                       FROM jbcity
                       WHERE name = „Los Angeles“);

ACHTUNG, immer wenn ich mehr als eine ID haben könnte, die machen könnte, dann IN, ansonsten
= (Where xx IN vs. Where xx =)




## 19b) because of the existence of foreign keys —> references to other tables etc.

#####20  # AS „test 1“ aber AS test reicht, nur für mehr als 1 Wort ist „“ notwendig

CREATE VIEW jbsale_supply(supplier, item, quantity) AS
SELECT supplier, item_name AS „item“, quality
FROM (SELECT jbsupplier.name AS „supplier“, jbitem.name AS „item_name“, jbitem.id
FROM jbsupplier, jbitem
WHERE jbsupplier.id=jbitem.supplier)
AS supplier_item
LEFT JOIN
jbsale ON supplier_item.id=jbsale.item;

























