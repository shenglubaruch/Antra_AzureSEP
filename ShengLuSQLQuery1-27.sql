--1.List of Persons’ full name, all their fax and phone numbers, as well as the phone number and fax of the company they are working for (if any).
SELECT
	ap.FullName, ap.PhoneNumber, ap.FaxNumber
FROM
	Application.People AS ap

UNION

SELECT
	sc.CustomerName, sc.PhoneNumber, sc.FaxNumber
FROM
	Sales.Customers AS sc
WHERE
	BuyingGroupID IS NULL;

--2.If the customer's primary contact person has the same phone number as the customer’s phone number, list the customer companies. 
SELECT 
	sc.CustomerName,
	SUBSTRING(
		sc.WebsiteURL, 
		CHARINDEX('.', sc.WebsiteURL)+1 , 
		LEN(sc.WebsiteURL)-CHARINDEX('.', sc.WebsiteURL)+1
		) AS Company
FROM
	Sales.Customers AS sc
INNER JOIN
	Application.People AS ap on sc.PrimaryContactPersonID = ap.PersonID
WHERE
	sc.PhoneNumber = ap.PhoneNumber;

--3.List of customers to whom we made a sale prior to 2016 but no sale since 2016-01-01.
SELECT DISTINCT
	sc.CustomerName
FROM
	Sales.Orders AS so
INNER JOIN
	Sales.Customers sc ON so.CustomerID = sc.CustomerID
WHERE so.OrderDate < '2016-01-01' 
	AND so.CustomerID NOT IN (
		SELECT DISTINCT so.customerID
		FROM Sales.Orders AS so
		WHERE so.OrderDate >= '2016-01-01'
	);

--4.List of Stock Items and total quantity for each stock item in Purchase Orders in Year 2013.
SELECT
	wst.StockItemID, ws.StockItemName, SUM(Quantity) AS TotalQuantity
FROM
	Warehouse.StockItemTransactions AS wst
INNER JOIN 
	Warehouse.StockItems AS ws ON wst.StockItemID = ws.StockItemID
WHERE
	year(TransactionOccurredWhen) = 2013
GROUP BY
	wst.StockItemID, ws.StockItemName;

--5.List of stock items that have at least 10 characters in description.
SELECT
	ppo.StockItemID, ppo.Description
FROM
	Purchasing.PurchaseOrderLines ppo
WHERE
	len(ppo.Description) >= 10;

--6.List of stock items that are not sold to the state of Alabama and Georgia in 2014.
SELECT DISTINCT 
	wst.StockItemID
FROM
	Warehouse.StockItemTransactions AS wst,
	Sales.Invoices AS si, Sales.Customers AS sc, Application.Cities AS ac
WHERE
	wst.InvoiceID = si.InvoiceID
	AND si.CustomerID = sc.CustomerID
	AND sc.DeliveryCityID = ac.StateProvinceID
	AND ac.StateProvinceID != 1
	AND ac.StateProvinceID != 11;

--7.List of States and Avg dates for processing (confirmed delivery date – order date).
SELECT
	asp.StateProvinceName, AVG(DATEDIFF(DAY, so.OrderDate, si.ConfirmedDeliveryTime)) AS AvgDatesForProcessing
FROM
	Sales.Orders AS so, Sales.Invoices AS si,
	Sales.Customers AS sc, Application.Cities AS ac,
	Application.StateProvinces AS asp
WHERE
	so.OrderID = si.OrderID
	AND si.CustomerID = sc.CustomerID
	AND sc.DeliveryCityID = ac.CityID
	AND ac.StateProvinceID = asp.StateProvinceID
GROUP BY asp.StateProvinceName;

--8.List of States and Avg dates for processing (confirmed delivery date – order date) by month.
SELECT
	asp.StateProvinceName, AVG(DATEDIFF(Month, so.OrderDate, si.ConfirmedDeliveryTime)) AS 'AvgDatesForProcessing(By Month)'
FROM
	Sales.Orders AS so, Sales.Invoices AS si,
	Sales.Customers AS sc, Application.Cities AS ac,
	Application.StateProvinces AS asp
WHERE
	so.OrderID = si.OrderID
	AND si.CustomerID = sc.CustomerID
	AND sc.DeliveryCityID = ac.CityID
	AND ac.StateProvinceID = asp.StateProvinceID
GROUP BY asp.StateProvinceName;

--9.List of StockItems that the company purchased more than sold in the year of 2015.
SELECT
	wst.StockItemID, ws.StockItemName, SUM(Quantity) AS TotalQuantity
FROM
	Warehouse.StockItemTransactions AS wst
INNER JOIN 
	Warehouse.StockItems AS ws ON wst.StockItemID = ws.StockItemID
WHERE
	year(TransactionOccurredWhen) = 2015 
GROUP BY
	wst.StockItemID, ws.StockItemName
HAVING
	SUM(Quantity) > 0;

--10.List of Customers and their phone number, together with the primary contact person’s name, to whom we did not sell more than 10 mugs (search by name) in the year 2016.
SELECT
	sc.CustomerName, sc.PhoneNumber, ap.FullName AS PrimaryContactName
FROM
	Sales.Customers AS sc
INNER JOIN 
	Application.People AS ap ON sc.PrimaryContactPersonID = ap.PersonID
WHERE
	sc.CustomerID NOT IN (
		SELECT 
			wst.CustomerID
		FROM 
			Warehouse.StockItemTransactions wst
		INNER JOIN 
			Warehouse.StockItemStockGroups wssg ON wst.StockItemID = wssg.StockItemID
		WHERE 
			wssg.StockGroupID = 3
			AND YEAR(TransactionOccurredWhen) = 2016 
		GROUP BY 
			wst.CustomerID
		HAVING 
			COUNT(wst.CustomerID) >10
);

--11.List all the cities that were updated after 2015-01-01.
SELECT
	Cityname
FROM
	Application.Cities
WHERE
	YEAR(ValidFrom) >= 2015;

--12.list all the Order Detail (Stock Item name, delivery address, delivery state, city, country, customer name, customer contact person name, customer phone, quantity) for the date of 2014-07-01. Info should be relevant to that date.
SELECT
	ws.StockItemName, si.DeliveryInstructions, ac.CityName, asp.StateProvinceName, actr.CountryName, sc.CustomerName, ap.FullName, sc.PhoneNumber, sol.Quantity
FROM
	Sales.Orders AS so, Sales.OrderLines AS sol,
	Sales.Invoices AS si, Sales.Customers AS sc,
	Warehouse.StockItems AS ws, Application.People AS ap,
	Application.Cities AS ac, Application.StateProvinces AS asp,
	Application.Countries As actr

WHERE
	so.OrderID = sol.OrderID
	AND si.OrderID = so.OrderID
	AND sol.StockItemID = ws.StockItemID
	AND so.CustomerID = sc.CustomerID
	AND so.ContactPersonID = ap.PersonID
	AND sc.DeliveryCityID = ac.CityID
	AND ac.StateProvinceID = asp.StateProvinceID
	AND asp.CountryID = actr.CountryID
	AND so.OrderDate = '2014-07-01';

--13.list of stock item groups and total quantity purchased, total quantity sold, and the remaining stock quantity (quantity purchased – quantity sold)
WITH SoldPurchase AS (
	SELECT wsg.StockGroupName, wst.Quantity, 
	CASE
		WHEN wst.PurchaseOrderID IS NULL THEN 'Sold'
		ELSE 'Purchase'
	END AS SoldOrPurchase
	FROM
		Warehouse.StockItemTransactions AS wst, Warehouse.StockGroups AS wsg,
		Warehouse.StockItemStockGroups as wssg
	WHERE
		wst.StockItemID = wssg.StockItemID
		AND wssg.StockGroupID = wsg.StockGroupID
)

SELECT
	wsg.StockGroupName,
	SUM(CASE WHEN SoldOrPurchase = 'Purchase' THEN SoldPurchase.Quantity ELSE 0 END) AS 'TotalQuantityPurchased',
	ABS(SUM(CASE WHEN SoldOrPurchase = 'Sold' THEN SoldPurchase.Quantity ELSE 0 END)) AS 'TotalQuantitySold',
	(SUM(CASE WHEN SoldOrPurchase = 'Purchase' THEN SoldPurchase.Quantity ELSE 0 END) + SUM(CASE WHEN SoldOrPurchase = 'Sold' THEN SoldPurchase.Quantity ELSE 0 END)) AS RemainingQuantity
FROM
	Warehouse.StockItemTransactions AS wst, Warehouse.StockItemStockGroups AS wssg,
	Warehouse.StockGroups AS wsg
INNER JOIN 
	SoldPurchase ON wsg.StockGroupName = SoldPurchase.StockGroupName
WHERE
	wst.StockItemID = wssg.StockItemID
	AND wssg.StockGroupID = wsg.StockGroupID

GROUP BY wsg.StockGroupName;

--14.List of Cities in the US and the stock item that the city got the most deliveries in 2016. If the city did not purchase any stock items in 2016, print “No Sales”.
WITH City AS (
	SELECT
		sc.DeliveryCityID, wst.StockItemID, SUM(Quantity) AS Quantity,
		ROW_NUMBER() OVER(PARTITION BY DeliveryCityID ORDER BY SUM(Quantity)) AS row_num
	FROM
		Sales.Customers AS sc
	INNER JOIN 
		Warehouse.StockItemTransactions AS wst ON sc.CustomerID = wst.CustomerID
	WHERE
		YEAR(wst.TransactionOccurredWhen) = 2016
		AND wst.CustomerID IS NOT NULL
	GROUP BY sc.DeliveryCityID, wst.StockItemID
)

SELECT 
	ac.CityName, ws.StockItemName, ABS(City.Quantity) AS MostDelivery
FROM
	Application.Cities AS ac 
LEFT JOIN
	City ON City.DeliveryCityID = ac.CityID
INNER JOIN
	Warehouse.StockItems AS ws ON City.StockItemID = ws.StockItemID
WHERE City.row_num = 1
ORDER BY ac.CityName;

--15.List any orders that had more than one delivery attempt (located in invoice table).
SELECT 
	OrderID, JSON_VALUE(ReturnedDeliveryData, '$.Events[1].Event') AS Attempt
FROM
	Sales.Invoices AS si
WHERE OrderID in (
	SELECT OrderID
	FROM Sales.Invoices
	GROUP BY OrderID
	HAVING COUNT(OrderID) > 1
)
ORDER BY si.OrderID;

--16.List all stock items that are manufactured in China. (Country of Manufacture).
--{ "CountryOfManufacture": "China", "Tags": ["USB Powered"] }
SELECT
	ws.StockItemName
FROM
	Warehouse.StockItems AS ws
WHERE
	JSON_VALUE(CustomFields, '$.CountryOfManufacture') = 'China';

--17.Total quantity of stock items sold in 2015, group by country of manufacturing.
SELECT
	JSON_VALUE(CustomFields, '$.CountryOfManufacture') AS Manufacturing, ABS(SUM(wst.Quantity)) AS TotalQuantitySold
FROM
	Warehouse.StockItems AS ws
INNER JOIN
	Warehouse.StockItemTransactions AS wst ON ws.StockItemID = wst.StockItemID
WHERE 
	PurchaseOrderID IS NULL
	AND YEAR(wst.TransactionOccurredWhen) = 2015
GROUP BY JSON_VALUE(CustomFields, '$.CountryOfManufacture');

--18.Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. [Stock Group Name, 2013, 2014, 2015, 2016, 2017]
CREATE VIEW TotalQuantity1 AS 
	SELECT 
		* 
	FROM (
		SELECT
			wsg.StockGroupName, YEAR(wst.TransactionOccurredWhen) AS 'Year', ABS(wst.Quantity) AS Quantity
		FROM
			Warehouse.StockItemTransactions AS wst, Warehouse.StockGroups AS wsg,
			Warehouse.StockItemStockGroups AS wssg
		WHERE
			wst.StockItemID = wssg.StockItemID
			AND wssg.StockGroupID = wsg.StockGroupID
			AND wst.PurchaseOrderID IS NULL
			AND YEAR(wst.TransactionOccurredWhen) BETWEEN 2013 AND 2017
		) AS t

		PIVOT (
			SUM(Quantity)
			FOR Year IN (
				[2013],
				[2014],
				[2015],
				[2016],
				[2017]
			)
		) AS pvt;

--19.Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. [Year, Stock Group Name1, Stock Group Name2, Stock Group Name3, … , Stock Group Name10]
CREATE VIEW TotalQuantity2 AS 	
	SELECT 
		[Year], [T-Shirts], [USB Novelties], [Packaging Materials], [Clothing], [Novelty Items], [Furry Footwear], [Mugs], [Toys], [Computing Novelties]
	FROM (
		SELECT
			wsg.StockGroupName, YEAR(wst.TransactionOccurredWhen) AS 'Year', ABS(wst.Quantity) AS Quantity
		FROM
			Warehouse.StockItemTransactions AS wst, Warehouse.StockGroups AS wsg,
			Warehouse.StockItemStockGroups AS wssg
		WHERE
			wst.StockItemID = wssg.StockItemID
			AND wssg.StockGroupID = wsg.StockGroupID
			AND wst.PurchaseOrderID IS NULL
			AND YEAR(wst.TransactionOccurredWhen) BETWEEN 2013 AND 2017
		) AS t

		PIVOT (
			SUM(Quantity)
			FOR StockGroupName IN (
				[T-Shirts],
				[USB Novelties],
				[Packaging Materials],
				[Clothing], [Novelty Items],
				[Furry Footwear],
				[Mugs],
				[Toys],
				[Computing Novelties]
			)
		) AS pvt;

--20.Create a function, input: order id; return: total of that order. List invoices and use that function to attach the order total to the other fields of invoices. 
CREATE FUNCTION dbo.ufnGetTotalOfOrder(@OrderID int)
RETURNS int
AS
BEGIN
	DECLARE @res int;
	SELECT
		@res = ISNULL(SUM(sol.Quantity * sol.UnitPrice * (100 + sol.TaxRate) / 100), 0)
	FROM 
		Sales.OrderLines AS sol
	WHERE sol.OrderID = @OrderID
	RETURN @res
END;

SELECT
	si.InvoiceID, dbo.ufnGetTotalOfOrder(si.OrderID) AS OrderTotal
FROM
	Sales.Invoices AS si
ORDER BY si.InvoiceID;

/*21.Create a new table called ods.Orders. Create a stored procedure, with proper error handling and transactions, that input is a date; 
when executed, it would find orders of that day, calculate order total, and save the information (order id, order date, order total, customer id) 
into the new table. If a given date is already existing in the new table, throw an error and roll back. Execute the stored procedure 5 times using different dates.*/
CREATE SCHEMA obs;
CREATE TABLE obs.Orders
(OrderID int, OrderDate date, OrderTotal int, CustomerID int);

IF OBJECT_ID('obs.OrderTotalOnThatDate', 'P') IS NOT NULL
	DROP PROCEDURE obs.OrderTotalOnThatDate;

CREATE PROCEDURE obs.OrderTotalOnThatDate
	@odate date
AS
BEGIN
	BEGIN TRANSACTION
		IF @odate NOT IN (SELECT OrderDate FROM obs.Orders)
			INSERT INTO obs.Orders
				(OrderID, OrderDate, OrderTotal, CustomerID)
			SELECT
				so.OrderID, so.OrderDate, so.CustomerID,
				SUM(sol.Quantity * sol.UnitPrice * (100 + sol.TaxRate) / 100)
			FROM
				Sales.Orders AS so
			INNER JOIN
				Sales.OrderLines AS sol ON so.OrderID = sol.OrderID
			WHERE so.OrderDate = @odate
			GROUP BY so.OrderID, so.OrderDate, so.CustomerID
		ELSE
			ROLLBACK TRANSACTION
	COMMIT TRANSACTION
END;

EXECUTE obs.OrderTotalOnThatDate '2013-01-01'
EXECUTE obs.OrderTotalOnThatDate '2013-01-02'
EXECUTE obs.OrderTotalOnThatDate '2013-01-03'
EXECUTE obs.OrderTotalOnThatDate '2013-01-04'
EXECUTE obs.OrderTotalOnThatDate '2013-01-05'

SELECT * FROM obs.Orders ORDER BY obs.Orders.OrderDate
TRUNCATE TABLE obs.Orders;

/*22.Create a new table called ods.StockItem. It has following columns: 
[StockItemID], [StockItemName] ,[SupplierID] ,[ColorID] ,[UnitPackageID] ,[OuterPackageID] ,[Brand] ,[Size] ,[LeadTimeDays] ,
[QuantityPerOuter] ,[IsChillerStock] ,[Barcode] ,[TaxRate]  ,[UnitPrice],[RecommendedRetailPrice] ,[TypicalWeightPerUnit] ,
[MarketingComments]  ,[InternalComments], [CountryOfManufacture], [Range], [Shelflife]. Migrate all the data in the original stock item table. */
SELECT 
	[StockItemID], [StockItemName], [SupplierID], [ColorID], [UnitPackageID], [OuterPackageID], [Brand], [Size], [LeadTimeDays], [QuantityPerOuter],
	[IsChillerStock], [Barcode], [TaxRate], [UnitPrice], [RecommendedRetailPrice], [TypicalWeightPerUnit], [MarketingComments], [InternalComments],
	JSON_VALUE(CustomFields, '$.CountryOfManufacture') AS [CountryOfManufacture], JSON_VALUE(CustomFields, '$.Range') AS [Range],
	JSON_VALUE(CustomFields, '$.ShelfLife') AS [ShelfLife]
INTO
	obs.StockItems
FROM
	Warehouse.StockItems

SELECT * FROM obs.StockItems;


/*23.Rewrite your stored procedure in (21). Now with a given date, it should wipe out all the order data 
prior to the input date and load the order data that was placed in the next 7 days following the input date. */
IF OBJECT_ID('obs.Next7daysOrderTotalOnThatDate', 'P') IS NOT NULL
	DROP PROCEDURE obs.Next7daysOrderTotalOnThatDate;

CREATE PROCEDURE obs.Next7daysOrderTotalOnThatDate
	@odate date
AS
BEGIN
	BEGIN TRANSACTION
		DELETE FROM obs.Orders
		WHERE obs.Orders.OrderDate < @odate

		INSERT INTO obs.Orders
			(OrderID, OrderDate, OrderTotal, CustomerID)
		SELECT
			so.OrderID, so.OrderDate, so.CustomerID,
			SUM(sol.Quantity * sol.UnitPrice * (100 + sol.TaxRate) / 100)
		FROM
			Sales.Orders AS so
		INNER JOIN
			Sales.OrderLines AS sol ON so.OrderID = sol.OrderID
		WHERE so.OrderDate >= @odate AND so.OrderDate < DATEADD(DAY, 7, @odate)
		GROUP BY so.OrderID, so.OrderDate, so.CustomerID
	COMMIT TRANSACTION
END;

EXECUTE obs.Next7daysOrderTotalOnThatDate '2013-01-01'
EXECUTE obs.Next7daysOrderTotalOnThatDate '2013-01-02'
EXECUTE obs.Next7daysOrderTotalOnThatDate '2013-01-03'
EXECUTE obs.Next7daysOrderTotalOnThatDate '2013-01-04'
EXECUTE obs.Next7daysOrderTotalOnThatDate '2013-01-05'

SELECT * FROM obs.Orders ORDER BY obs.Orders.OrderDate
TRUNCATE TABLE obs.Orders;

--24.Looks like that it is our missed purchase orders. Migrate these data into Stock Item, Purchase Order and Purchase Order Lines tables. Of course, save the script.
DECLARE @json NVARCHAR(4000) = '{
   "PurchaseOrders":[
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"7",
         "UnitPackageId":"1",
         "OuterPackageId":[6,7],
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-01",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"WWI2308"
      },
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"5",
         "UnitPackageId":"1",
         "OuterPackageId":"7",
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-025",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"269622390"
      }
   ]
}'
SELECT * INTO obs.temp
FROM OPENJSON(@json, '$.PurchaseOrders')
WITH (
	StockItemName			NVARCHAR(100)	'$.StockItemName',
	SupplierID				INT				'$.Supplier',
	UnitPackageID			INT				'$.UnitPackageId',
	OuterPackageID			INT				'$.OuterPackageId',
	Brand					NVARCHAR(50)	'$.Brand',
	LeadTimeDays			INT				'$.LeadTimeDays',
	QuantityPerOuter		INT				'$.QuantityPerOuter',
	TaxRate					DECIMAL(18,3)	'$.TaxRate',
	UnitPrice				DECIMAL(18,2)	'$.UnitPrice',
	RecommendedRetailPrice	DECIMAL(18,2)	'$.RecommendedRetailPrice',
	TypicalWeightPerUnit	DECIMAL(18,3)	'$.TypicalWeightPerUnit'
)
INSERT INTO
	Warehouse.StockItems(
	StockItemName,
	SupplierID,
	UnitPackageID,
	OuterPackageID,
	Brand,
	LeadTimeDays,
	QuantityPerOuter,
	TaxRate,
	UnitPrice,
	RecommendedRetailPrice,
	TypicalWeightPerUnit
	) 
SELECT * FROM obs.temp
DROP TABLE obs.temp
SELECT * FROM obs.temp
SELECT * FROM Warehouse.StockItems

--25.Revisit your answer in (19). Convert the result in JSON string and save it to the server using TSQL FOR JSON PATH.
SELECT * FROM TotalQuantity2  FOR JSON PATH

--26.Revisit your answer in (19). Convert the result into an XML string and save it to the server using TSQL FOR XML PATH.
SELECT
	year, [T-Shirts],
	[USB Novelties] AS USBNovelties, 
	[Packaging Materials] AS PackagingMaterials,
	Clothing,
	[Novelty Items] AS NoveltyItems,
	[Furry Footwear] AS FurryFootwear,
	Mugs,
	Toys,
	[Computing Novelties] AS ComputingNovelities
FROM TotalQuantity2  FOR XML PATH

/*27.Create a new table called ods.ConfirmedDeviveryJson with 3 columns (id, date, value).
Create a stored procedure, input is a date. The logic would load invoice information (all columns) 
as well as invoice line information (all columns) and forge them into a JSON string and then insert 
into the new table just created. Then write a query to run the stored procedure for each DATE that
customer id 1 got something delivered to him.*/
CREATE TABLE obs.ConfirmedDeviveryJson
(id int, [date] date, [value] NVARCHAR(MAX));

IF OBJECT_ID('obs.DeliveryJson', 'P') IS NOT NULL
	DROP PROCEDURE obs.DeliveryJson;

CREATE PROCEDURE obs.DeliveryJson
	@idate date,
	@customerid int
AS
BEGIN
	INSERT INTO
		obs.ConfirmedDeviveryJson
	SELECT
		si.CustomerID, si.InvoiceDate, 
		(SELECT * FROM Sales.Invoices AS si
		INNER JOIN Sales.InvoiceLines AS sil ON si.InvoiceID = sil.InvoiceID
		WHERE Si.InvoiceDate = @idate
		FOR JSON AUTO)
	FROM 
		Sales.Invoices AS si
	INNER JOIN
		Sales.InvoiceLines AS sil ON si.InvoiceID = sil.InvoiceID 
	WHERE
		si.InvoiceDate = @idate
		AND si.CustomerID = @customerid
END;

EXECUTE obs.DeliveryJson '2013-03-04', 1

SELECT * FROM obs.ConfirmedDeviveryJson
TRUNCATE TABLE obs.ConfirmedDeviveryJson
DROP TABLE obs.ConfirmedDeviveryJson


