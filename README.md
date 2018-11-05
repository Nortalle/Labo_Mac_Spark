# Labo Mac

http://spark.apache.org/docs/2.2.2/api/java/org/apache/spark/storage/RDDInfo.html

https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations

[TOC]

## Ex1 - Print the movies whose title contains "Avengers" 

Goal: 

* use `map()` and `filter()` methods to get the title of the movies that contains "Avengers" in their title

Output example:

```plain
The Avengers 
Avengers: Age of Ultron
```

Steps:

* Use `filter()` to only keep the movies that contains "Avengers" in their title
* Use `map()` to retrieve the titles of these filtered movies
* Use `foreach()` to pretty print the results

###Solution

```scala
rddMovies.filter(_.title.contains("Avengers")).collect().map(m => m.title).foreach(println)
```

## Ex2 - Print movies title released between yearMin and yearMax

Goal:
​    
* Print the titles of the movies that were released between `yearMin` and `yearMax` (including `yearMin` and `yearMax`).
* This list is sorted by year desc

Output example:

```plain
2017 - Harry Potter
2015 - American Pie
2014 - Matrix
```

Steps:

* Use `filter()` to only keep the movies released between `yearMin` and `yearMax`
* Sort the filtered movies by decreasing year
* Use `map()` to keep only the relevant attributes (i.e. year and title)
* Use `foreach()` to pretty print the results

### Solution

```scala
rddMovies.filter(_.title.contains("Avengers")).collect().map(m => m.title).foreach(println)
```

```scala
def ex2(yearMin :Int, yearMax: Int){
    rddMovies.filter((x$1) => yearMin.$less$eq(x$1.year)).filter((x$1) => yearMax.$greater$eq(x$1.year)).map(m => (m.year, m.title)).sortByKey(false).collect.foreach(m => println(m._1 + " - " + m._2))
}

ex2(2008, 2008)
```



## Ex3 - Print average rating per year

Goal:

* Print the average rating per year
* This output is sorted by increasing year

Output example:

```plain
year: 2008 average rating: 2.13456
year: 2009 average rating: 4.456789
year: 2010 average rating: 6.76543
```

Theory:

We are going to use `reduceByKey()` which has the following signature `reduceByKey(func: (V, V) => V): RDD[(K, V)]`. 

`reduceByKey()` works on a RDD like `RDD[(K,V)]` (i.e. sort of "list of key/values pairs"). 

`reduceByKey()` takes a function that, from two elements, returns one i.e. the `func: (V, V) => V` in the signature.
The difference with `reduce()` is that `reduceByKey()` uses two elements sharing the same key.

For example (pseudo code):

```plain
 year, count
(2010, 2)
(2011, 3)
(2011, 4)
(2010, 8)
// use reduceByKey((count1, count2) => count1+count2)
> (2010, 10)
> (2011, 7)
```

Note: here `count` is just an Int but it can be anything e.g. `Movie`

Steps:

* To compute the average we need the **total sum** of rating per year and the **count** of all the movies per year
* Use `map()` to create an RDD made of `(year, (rating, 1))`. Like a word count we use the `1` to be able to count the number of movies per year
* Use `reduceByKey()` to sum the rating and to count the number of movies per year. The output should look like: `(year, (totalRating, moviePerYearCount))`
* Find a way to compute the average using the result from the last operation

###Solution

```Scala
rddMovies.map(m => (m.year, (m.rating, 1))).reduceByKey((V,W) => (V._1 + W._1, V._2 + W._2)).map(V => (V._1, V._2._1 / V._2._2)).sortByKey(true).collect().foreach(println)
```

%md 

## Ex4 - Print top actors

Goals:

* Print the list of the actors that appears the most
* Use `flatMap()`

Output example:

```plain
Top actors:
Brad Pitt (18)
Tom Hardy (4)
Anna Kendrick (4)
Celion Dion (1)
```

Theory:

When an operation is giving you a sequence of sequences like:

```scala
Array("hello", "world").map(word => word.split(""))
res91: Array[Array[String]] = Array(Array(h, e, l, l, o), Array(w, o, r, l, d))
```

You may want to flatten this to only have a single list like:
```scala
Array("hello", "world").map(_.split("")).flatten
res93: Array[String] = Array(h, e, l, l, o, w, o, r, l, d)
```

You can achieve the same result (i.e. `map` + `flatten`) using `flatMap`:
```scala
Array("hello", "world").flatMap(_.split(""))
res95: Array[String] = Array(h, e, l, l, o, w, o, r, l, d)
```

We are going to apply this same technique with the `actors` member.

Steps:

* Use `flatMap()` to get the list with all the actors
* Make sure to remove trailling whitespaces
* Count the actors
* Sort them by decreasing order
* Show the top N actors

###Solution

```scala
// TODO student
def printTopNactors(n: Int) {
    rddMovies.flatMap(m => m.actors).map(a => (a.trim , 1)).reduceByKey((V,W) => (V + W)).sortBy(_._2, false).take(n).foreach(a => println(a._1 + " (" + a._2 + ")"))
}

printTopNactors(90)

```