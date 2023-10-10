# Data Warehouse
This repository contains an implementation of a Data Warehouse created for the retail business analytics purposes. The data represents various aspects of the retail sales operations on Amazon web-site, including the customer, employee, product, warehouse, shipping, price, description, discount, and so on. The architechure of the DWH contains the staging area, where the data is preprocessed, then the data goes to the 3NF layer, where it is stored in a normilized way. Further, the data flows go to the dimentional layer where the star schema is populated. The implementation includes slowly changing dimentions (SCD) type 1 and 2 to track the historical changes.  

Data loading into the DWH is performed using full load (with loading of the whole dataset) and incremental load (the chunks of the data are loaded more frequently) strategies. In order to make the process more efficient, data partitioning is used as well. 

The project is developed with PostgreSQL using mostly PL/pgSQL to create, link and optimize all the necessary objects and processes.    

The created Data Warehouse serves for the purposes of business intelligence (BI). The raw business operations data can be transformed to the meaningful analytical reports and support the management decision making.

3NF layer:
![3NF](https://github.com/alexeyburtsev/Projects/assets/77303244/a1c20efe-0f45-4fea-9589-5cc7129016d2)

Dimentional layer:
![STAR](https://github.com/alexeyburtsev/Projects/assets/77303244/57b6c531-a528-447b-8e96-181c8b563688)
