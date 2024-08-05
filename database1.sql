cur.execute("""
                CREATE TABLE IF NOT EXISTS File1 (
                     name VARCHAR(1000) NOT NULL,
                     phonenumber INT,
                     email VARCHAR(1000),
                     position VARCHAR(1000),
                     salary DECIMAL(10, 2)
                );
                """)
                
                cur.execute("""
                CREATE TABLE IF NOT EXISTS File2 (
                    id SERIAL PRIMARY KEY,
                    productname VARCHAR(5000),
                    productprice INT,    
                    salespersonid INT,
                    legalpersonid INT,
                    developerid INT,
                    testerid INT,
                    hrid INT,
                    FOREIGN KEY (salespersonid) REFERENCES File1(id),
                    FOREIGN KEY (legalpersonid) REFERENCES File1(id),
                    FOREIGN KEY (developerid) REFERENCES File1(id),
                    FOREIGN KEY (testerid) REFERENCES File1(id),
                    FOREIGN KEY (hrid) REFERENCES File1(id)
                );
                """)