CREATE TABLE deaian.trsh_selling_manager(
	seller_id INT CONSTRAINT pk_trsh_1 NOT NULL UNIQUE PRIMARY KEY AUTO_INCREMENT,
	name TEXT,
	surname TEXT,
	email TEXT CHECK(email LIKE '%@%'),
	PRIMARY KEY(seller_id)
	);
	
CREATE TABLE deaian.trsh_customer_info(
	cust_id INT CONSTRAINT pk_trsh_2 NOT NULL UNIQUE PRIMARY KEY AUTO_INCREMENT,
	name TEXT,
	surname TEXT,
	phone_number INT,
	email TEXT CHECK(email LIKE '%@%'),
	is_regular_customer CHAR(1),
	PRIMARY KEY(cust_id)
	);

CREATE TABLE deaian.trsh_auto_info(
	auto_id INT CONSTRAINT pk_trsh_3 NOT NULL UNIQUE PRIMARY KEY AUTO_INCREMENT,
	brand TEXT,
	model TEXT,
	production_year INT,
	color TEXT,
	status TEXT DEFAULT 'on sale',
	selling_price DECIMAL (10,2) NOT NULL,
	PRIMARY KEY(auto_id)
	);
	
CREATE TABLE deaian.trsh_selling_info(
	deal_id INT CONSTRAINT pk_trsh_4 NOT NULL UNIQUE PRIMARY KEY AUTO_INCREMENT,
	auto_id INT FOREIGN KEY REFERENCES auto_id,
	cust_id INT FOREIGN KEY REFERENCES cust_id,
	seller_id INT FOREIGN KEY REFERENCES seller_id,
	sale_price DECIMAL (10,2) NOT NULL,
	discount_amt DECIMAL (10,2) NOT NULL,
	sale_date DATE DEFAULT CURRENT_DATE
	);
	
ALTER TABLE deaian.trsh_selling_info
	ADD CONSTRAINT fk_trsh_1 
		FOREIGN KEY (seller_id)
		REFERENCES deaian.trsh_selling_manager (seller_id);

ALTER TABLE deaian.trsh_selling_info
	ADD CONSTRAINT fk_trsh_2 
		FOREIGN KEY (auto_id)
		REFERENCES deaian.trsh_auto_info (auto_id);	
		
ALTER TABLE deaian.trsh_selling_info
	ADD CONSTRAINT fk_trsh_3
		FOREIGN KEY (cust_id)
		REFERENCES deaian.trsh_customer_info (cust_id);	
		
ALTER TABLE deaian.trsh_customer_info
	ADD CONSTRAINT fk_trsh_4
		FOREIGN KEY (seller_id)
		REFERENCES  deaian.trsh_selling_manager (seller_id);	
	
INSERT INTO deaian.trsh_selling_manager(seller_id, name, surname, email)
VALUES(1, 'severus', 'snape', 'snape@auto.com'), 
	  (2, 'albus', 'dumbledore', 'dumbledore@auto.com');

INSERT INTO deaian.trsh_auto_info(auto_id, brand, model, production_year, color, status, selling_price)
VALUES(1, 'BMW', 'X5', 2010, 'black', 'on repair', 4000.00),
	  (2, 'BMW', 'X6', 2009, 'white', 'sold', 8000.00);


INSERT INTO deaian.trsh_customer_info(cust_id, seller_id, name, surname, phone_number, email, is_regular_customer)
VALUES (1,3,  'draco', 'malfoy', 88005553535, 'zabini@gmail.com', 1),
	   (2, 1, 'sirius', 'black', 88005553535, 'blackblack@dog.com', 0);
	   
	
INSERT INTO deaian.trsh_selling_info(deal_id, auto_id, cust_id, seller_id, sale_price, discount_amt, sale_date)
VALUES(1, 4, 1, 3, 6300.00, 0.05, to_date('2023-06-20', 'YYYY-MM-DD')), 
	  (2, 3, 2, 1, 8000.00, 0.00, to_date('2023-06-25', 'YYYY-MM-DD'));

	  