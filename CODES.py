#Single coloum Filter Function: (using df table)
#SQL:-
spark.sql("select * from df where category = 'Exercise'").show()
#DFS:-
fildf= df.filter("category = 'Exercise'").show()
#Two Coloum Filter “AND” Function: (using df table)
#SQL:-
spark.sql("select id,tdate,category,spendby from df where category = 'Exercise' and spendby = 'cash'").show()
#DFS:-
fildf= df.filter("category = 'Exercise' and spendby ='cash'").select("id","tdate","category","spendby").show()
#Multi Filter “IN” Function: (using df table)
#SQL:-
spark.sql("select * from df where category in ('Exercise', 'Gymnastics')").show()
#DFS:-
fildf= df.filter("category in ('Exercise','Gymnastics')")
fildf.show()
#Filter “LIKE” Function: (using df table)
#SQL:-
spark.sql("select * from df where product like  '%Gymnastics%' ").show()
#DFS:-
fildf= df.filter("product like '%Gymnastics%' ").show()
#Filter “NOT EQUAL TO” Function: (using df table)
#SQL:-
spark.sql("select * from df where category != 'Exercise'").show()
#DFS:- 
fildf= df.filter("category != 'Exercise'").show()
#Multi Filter “NOT IN” Function: (using df table)
#SQL:-
spark.sql("select * from df where category not in ('Exercise','Gymnastics')").show()
#DFS:-
fildf= df.filter("category not in ('Exercise','Gymnastics')")
fildf.show()
#Filter “IS” Function: (using df table)
#SQL:-
spark.sql("select * from df where product is null").show()
#DFS:-
fildf= (df.filter("product is null")).show()
#Filter “IS NOT” Function: (using df table)
#SQL:-
spark.sql("select * from df where product is not null").show()
#DFS:-
fildf= (df.filter("product is not null")).show()
#Aggragition “MAX” Function: (using df table)
#SQL:-
spark.sql("select max(id) as idmax from df ").show()
#DFS:-
idmax = df.agg({"id": "max"}).collect()[0][0]
print(idmax)
#Aggragition “MIN” Function: (using df table)
#SQL:- 
spark.sql("select min(id) as idmin from df ").show()
#DFS:- 
idmin = df.agg({"id": "min"}).collect()[0][0]
print(idmin)
#Aggragition “COUNT” Function: (using df table)
#SQL:- 
spark.sql("select count(1) as count from df ").show()
#DFS:- 
count = df.count()
print(count)
#Aggragition “CONDITIONAL STATEMENT” Function: (using df table)
#SQL:- 
spark.sql("select *, CASE WHEN spendby = 'cash' THEN '1' ELSE '0' END as status from df ").show()
#DFS:- 
status = df.withColumn("status", when(col("spendby") == 'cash', '1').otherwise('0'))
status.show()
#Aggragition “CONCATE STATEMENT” Function: (using df table)
#SQL:- 
spark.sql("select id,category,concat(id,'-',category) as conct from df ").show()
#DFS:- 
conct = df. df.select("id", "category", concat(col("id"), lit("-"), col("category")).alias("concat"))
conct.show()
#Aggragition Multi “CONCATE STATEMENT” Function: (using df table)
#SQL:- 
spark.sql("select id,category,product,concat_ws('-',id,category,product) as conct from df ").show()
#DFS:- 
conct = df.select("id", "category","product", concat_ws("-",col("id"), col("category"),col("product")).alias("concat"))
conct.show()
#Aggragition  “LOWER” Function: (using df table)
#SQL:- 
spark.sql("select category,lower(category) as lower from df ").show()
#DFS:- 
lower = df.select( "category", lower("category").alias("lower")).show()
#Aggragition  “UPPER” Function: (using df table)
#SQL:- 
spark.sql("select category,upper(category) as upper from df ").show()
#DFS:- 
upper = df.select( "category", upper("category").alias("upper")).show()
#Ceil  Operator: (using df table)
#SQL:- 
spark.sql("select amount,ceil(amount) as ceil from df ").show()
#DFS:- 
Ceil = df.select( "amount", ceil("amount").alias("ceil")).show()
#Round  Operator: (using df table)
#SQL:- 
spark.sql("select amount,round(amount) as round from df ").show()
#DFS:- 
Round = df.select( "amount", round("amount").alias("round")).show()
#Replace Null “COALESCE” Function: (using df table)
#SQL:- 
spark.sql("select product,coalesce(product,'NA') as nullrep from df ").show()
#DFS:- 
nullreplace = df.select("product", coalesce(col("product"), lit("NA")).alias("nullrep")).show()
#“TRIM” Function: (using df table)
#SQL:- 
spark.sql("select product,trim(product) as trim from df ").show()
#DFS:- 
trimdf = df.select("product", trim("product").alias("trim")).show()
#Filter “DISTINCT” Function: (using df table)
#SQL:- 
spark.sql("select distinct category as dis from df ").show()
#DFS:- 
disdf = df.select(col("category").alias("dis")).distinct().show()
#Multi Filter “DISTINCT” Function: (using df table)
#SQL:- 
spark.sql("select distinct category,spendby as dis from df ").show()
#DFS:- 
disdf = df.select("category", "spendby").distinct()
disdf.show()
#Filter “SUBSTRING” Function: (using df table)
#SQL:- 
spark.sql("select substring(product,1,10) as substring from df ").show()
#DFS:- 
substring = df.select(substring(col("product"), 1, 10).alias("substring"))
substring.show()
#Filter “SPLIT” Function: (using df table)
#SQL:- 
spark.sql("select product,split(product, ' ')[0] as split from df ").show()
#DFS:- 
split = df.select("product",split(col("product"), " ")[0].alias("split"))
split.show()
#“UNION ALL” Function: (using df & df1 table)
#SQL:- 
spark.sql("select * from df union all select * from df1 ").show()
#DFS:- 
union_df = df.unionAll(df1)
union_df.show()
#“UNION” Function: (using df & df1 table)
#SQL:- 
spark.sql("select * from df union select * from df1 ").show()
#DFS:- 
union_df = df.union(df1).distinct()
union_df.show()
#Aggregation “SUM” Function: (using df table)
#SQL:- 
spark.sql("select category,sum(amount) as sum from df group by category ").show()
#DFS:- 
sum_df = df.groupBy("category").agg(sum("amount").alias("sum")).show()
#Aggregation “SUM” Function Multi Coloum: (using df table)
#SQL:- 
spark.sql("select category, spendby,sum(amount) as sum from df group by category, spendby ").show()
#DFS:- 
sum_df = df.groupBy("category", "spendby").agg(sum("amount").alias("sum")).show()
#Aggregation “SUM + COUNT” Function Multi Coloum: (using df table)
#SQL:- 
spark.sql("select category, spendby,sum(amount) as sum, count(amount) as count from df group by category, spendby ").show()
#DFS:- 
sum_df = df.groupBy("category", "spendby").agg(sum("amount").alias("sum"), count("amount").alias("count"))
sum_df.show()
#Aggregation “MAX” Function : (using df table)
#SQL:- 
spark.sql("select category,max(amount) as max from df group by category").show()
#DFS:- 
max_df = df.groupBy("category").agg(max("amount").alias("max"))
max_df.show()
#Aggregation “MIN” Function : (using df table)
#SQL:- 
spark.sql("select category,min(amount) as min from df group by category").show()
#DFS:- 
min_df = df.groupBy("category").agg(min("amount").alias("min"))
min_df.show()
#Aggregation “MIN + ORDER BY” Function : (using df table)
#SQL:- 
spark.sql("select category,min(amount) as min from df group by category order by category").show()
#DFS:- 
order_df = df.groupBy("category").agg(min("amount").alias("min")).orderBy("category")
order_df.show()
#Window“ROW” Function: (using df table)
#SQL:- 
spark.sql("select category,amount,row_number() over (partition by category order by amount desc) as row from df").show()
#DFS:- 
from pyspark.sql.window import Window
window_spec = Window.partitionBy("category").orderBy(col("amount").desc())
Window_df = df.withColumn("row", row_number().over(window_spec)).select("category","amount","row")
Window_df.show()
#Window“DENSE_RANK” Function: (using df table)
#SQL:- 
spark.sql("select category,amount,dense_rank() over (partition by category order by amount desc) as dense_rank from df").show()
#DFS:- 
from pyspark.sql.window import Window
window_spec = Window.partitionBy("category").orderBy(col("amount").desc())
Window_df = df.withColumn("dense_rank", dense_rank().over(window_spec)).select("category","amount","dense_rank")
Window_df.show()
#Window“RANK” Function: (using df table)
#SQL:- 
spark.sql("select category,amount,rank() over (partition by category order by amount desc) as rank from df").show()
#DFS:- 
from pyspark.sql.window import Window
window_spec = Window.partitionBy("category").orderBy(col("amount").desc())
Window_df = df.withColumn("rank", rank().over(window_spec)).select("category","amount","rank")
Window_df.show()
#Window“LEAD” Function: (using df table)
#SQL:- 
spark.sql("select category,amount,lead(amount) over (partition by category order by amount desc) as lead_amount from df").show()
#DFS:- 
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lead
window_spec = Window.partitionBy("category").orderBy(col("amount").desc())
Window_df = df.withColumn("lead_amount", lead(col("amount")).over(window_spec)).select("category", "amount", "lead_amount")
Window_df.show()
#Window“LAG” Function: (using df table)
#SQL:- 
spark.sql("select category,amount,lag(amount) over (partition by category order by amount desc) as lag_amount from df").show()
#DFS:- 
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag
window_spec = Window.partitionBy("category").orderBy(col("amount").desc())
Window_df = df.withColumn("lag_amount", lag(col("amount")).over(window_spec)).select("category", "amount", "lag_amount")
Window_df.show()
#“HAVING” Function: (using df table) {Only in SQL}
#SQL:- 
spark.sql("select category,count(category) as cnt from df group by category having count(category) > 1").show()
#DFS:- 
having_df = df.groupBy("category").agg(count("category").alias("cnt")).filter("cnt > 1").select("category", "cnt")
having_df.show()
#Join “INNER” Function: (using cust , prod table)
#SQL:- 
spark.sql("select a.*, b.product from cust a join prod b on a.id=b.id ").show()
#DFS:- 
join = cust.join(prod, ["id"], "inner").show()
#Join “LEFT” Function: (using cust , prod table)
#SQL:- 
spark.sql("select a.*, b.product from cust a left join prod b on a.id=b.id").show()
#DFS:- 
join = cust.join(prod, ["id"], "left").show()
#Join “RIGHT” Function: (using cust , prod table)
#SQL:- 
spark.sql("select a.*, b.product from cust a right join prod b on a.id=b.id").show()
#DFS:- 
join = cust.join(prod, ["id"], "right").show()
#Join “FULL” Function: (using cust , prod table)
#SQL:- 
spark.sql("select a.*, b.product from cust a full join prod b on a.id=b.id").show()
#DFS:- 
join = cust.join(prod, ["id"], "full").show()
#Join “Left ANTI” Function: (using cust , prod table)
#SQL:- 
spark.sql("select a.* from cust a left anti join prod b on a.id=b.id").show()
#DFS:- 
join = cust.join(prod, ["id"], "leftanti").show()
#Join “CROSS” Function: (using cust , prod table)
#SQL:- 
spark.sql("select a.*,b.* from cust a cross join prod b").show()
#DFS:- 
join = cust.crossJoin(prod).show()
