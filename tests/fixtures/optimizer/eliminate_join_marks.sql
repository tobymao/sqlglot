 
# title: Constant in join  
# dialect: oracle
SELECT T1.d, T2.c FROM T1, T2 WHERE T1.x = T2.x (+) and T2.y (+) > 5;
SELECT T1.d, T2.c FROM T1 LEFT JOIN T2 ON T1.x = T2.x AND T2.y > 5;
  
# title: Null in Join Predicte  
# dialect: oracle
SELECT T1.d, T2.c FROM T1, T2 WHERE T1.x = T2.x (+) and T2.y (+) IS NULL;
SELECT T1.d, T2.c FROM T1 LEFT JOIN T2 ON T1.x = T2.x AND T2.y IS NULL;
  
# title: Null in Join Predicte 2  
# dialect: oracle
SELECT T1.d, T2.c FROM T1, T2 WHERE T1.x = T2.x (+) and T2.y IS NULL;
SELECT T1.d, T2.c FROM T1 LEFT JOIN T2 ON T1.x = T2.x WHERE T2.y IS NULL;
  
# title: Additional where clause  
# dialect: oracle
SELECT T1.d, T2.c FROM T1, T2 WHERE T1.x = T2.x (+) and T1.Z > 4;
SELECT T1.d, T2.c FROM T1 LEFT JOIN T2 ON T1.x = T2.x WHERE T1.Z > 4;
  
# title: various select tests 1  
# dialect: oracle
SELECT * FROM table1, table2 WHERE table1.column = table2.column(+);
SELECT * FROM table1 LEFT JOIN table2 ON table1.column = table2.column;
  
# title: various select tests 2  
# dialect: oracle
SELECT * FROM table1, table2, table3, table4 WHERE table1.column = table2.column(+) and table2.column >= table3.column(+) and table1.column = table4.column(+);
SELECT * FROM table1 LEFT JOIN table2 ON table1.column = table2.column LEFT JOIN table3 ON table2.column >= table3.column LEFT JOIN table4 ON table1.column = table4.column;
  
# title: various select tests 3  
# dialect: oracle
SELECT * FROM table1, table2, table3 WHERE table1.column = table2.column(+) and table2.column >= table3.column(+);
SELECT * FROM table1 LEFT JOIN table2 ON table1.column = table2.column LEFT JOIN table3 ON table2.column >= table3.column;
    
# title: subquery  
# dialect: oracle 
SELECT table1.id, table2.cloumn1, table3.id FROM table1, table2, (SELECT tableInner1.id FROM tableInner1, tableInner2 WHERE tableInner1.id = tableInner2.id(+)) AS table3 WHERE table1.id = table2.id(+) and table1.id = table3.id(+);
SELECT table1.id, table2.cloumn1, table3.id FROM table1 LEFT JOIN table2 ON table1.id = table2.id LEFT JOIN (SELECT tableInner1.id FROM tableInner1 LEFT JOIN tableInner2 ON tableInner1.id = tableInner2.id) table3 ON table1.id = table3.id;

