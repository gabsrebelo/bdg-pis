from pyspark import SparkConf, SparkContext
import argparse
import json
from pyspark.sql import Row, SQLContext
from pyspark.sql.functions import desc, asc

conf = SparkConf().setMaster("local").setAppName("BD")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

argparser = argparse.ArgumentParser()
argparser.add_argument("--input", help="input file with products")
argparser.add_argument("-f","--file",default="products.txt",help="name of the file to write products dicts")
args = argparser.parse_args()

def parse(filename, total):
    '''
    Parser que le o .txt e a saida eh um generator dos produtos na estrutura de dicionario
    '''
    IGNORE_FIELDS = ['Total items', 'reviews']
    f = open(filename, 'r')
    lines = f.readlines()
    entry = {}
    categories = []
    reviews = []
    similar_items = []
  
    for line in lines[0:total]:
        colonPos = line.find(':')

        if line.startswith("Id"):
            if reviews:
                entry["reviews"] = reviews
            if categories:
                entry["categories"] = categories
            yield entry
            entry = {}
            categories = []
            reviews = []
            rest = line[colonPos+2:]
            entry["id"] = rest[1:-1]
      
        elif line.startswith("similar"):
            similar_items = line.split()[2:]
            entry['similar_items'] = similar_items

    # "cutomer" is typo of "customer" in original data
        elif line.find("cutomer:") != -1:
            review_info = line.split()
            reviews.append({'customer_id': review_info[2], 
                          'rating': int(review_info[4]), 
                          'votes': int(review_info[6]), 
                          'helpful': int(review_info[8]),
                           'date':review_info[0]})

        elif line.startswith("   |"):
            categories.append(line[0:-1].replace(' ',''))

        elif colonPos != -1:
            eName = line[:colonPos]
            rest = line[colonPos+2:]
            if not eName in IGNORE_FIELDS:
                if eName[0] == ' ':
                    eName = eName[2:]
                entry[eName] = rest[0:-1].replace("'","''")

    if reviews:
        entry["reviews"] = reviews
    if categories:
        entry["categories"] = categories
    
    yield entry

def makeFile(file_path):
    line_num = sum(1 for line in open(file_path))
    f=open(args.file,"a+")
    for e in parse(file_path, total=line_num):
        if e:
            json.dump(e,f)
            f.write("\n")
    f.close()

makeFile(args.input)

#mudar para args.file depois
products = sc.textFile(args.file) \
            .map(lambda x: json.loads(x))\
            .filter(lambda x: "group" in x) #serve para eliminar linhas que contem apenas id e ASIN

#Criando dataframe de reviews
def createDF(x):
    rows = []
    for r in x["reviews"]:
        rows.append(Row(asin=x["ASIN"],date=r["date"],rating=r["rating"],votes=r["votes"],\
                        customer_id=r["customer_id"],helpful=r["helpful"]))
    return rows

reviews = products.filter(lambda x: "reviews" in x and isinstance(x["reviews"],list))\
                    .flatMap(createDF)
reviewsDF = sqlContext.createDataFrame(reviews)

#Criando dataframe de produtos
productsRow = products.map(lambda x: Row(asin=x["ASIN"],group=x["group"],title=x["title"],salesrank=x["salesrank"],\
    id=x["id"],categories=x["categories"]))    
productsDF = sqlContext.createDataFrame(productsRow)

def getHelpfulReviews(asin):    
    reviewsDF.select("*")\
            .where("asin = "+asin)\
            .orderBy(desc("rating"),desc("helpful"))\
            .show(5)

    reviewsDF.select("*")\
            .where("asin = "+asin)\
            .orderBy(asc("rating"),desc("helpful"))\
            .show(5)

def getTop10ProductsOfEachGroup():
    productsDF.select("*").where("group = 'Book'").orderBy(asc("salesrank")).show(10)
    productsDF.select("*").where("group = 'DVD'").orderBy(asc("salesrank")).show(10)
    productsDF.select("*").where("group = 'Music'").orderBy(asc("salesrank")).show(10)
    productsDF.select("*").where("group = 'Video'").orderBy(asc("salesrank")).show(10)

def getTop10ProductsWithMostRatingAVG():
    reviewsDF.groupBy("asin").agg({'rating': 'mean'}).orderBy(desc("avg(rating)")).show(10)

def getTop5CategoriesWithMostRating():
    joinExpression = productsDF["asin"] == reviewsDF["asin"]
    productsDF.join(reviewsDF,joinExpression)\
        .groupBy("categories")\
        .agg({'rating':'mean'})\
        .orderBy(desc("avg(rating)"))\
        .show(5)

def get10CustomersWithMostReviewsForEachGroup():
    joinExpression = productsDF["asin"] == reviewsDF["asin"]

    print("os 10 clientes que mais fizeram comentarios por livros\n")
    productsDF.join(reviewsDF,joinExpression)\
        .where("group = 'Book'")\
        .groupBy("customer_id")\
        .count()\
        .orderBy(desc("count"))\
        .show()
    
    print('\nos 10 clientes que mais fizeram comentarios por DVDs\n')

    productsDF.join(reviewsDF,joinExpression)\
        .where("group = 'DVD'")\
        .groupBy("customer_id")\
        .count()\
        .orderBy(desc("count"))\
        .show()

    print('\nos 10 clientes que mais fizeram comentarios por musicas\n')
    productsDF.join(reviewsDF,joinExpression)\
        .where("group = 'Music'")\
        .groupBy("customer_id")\
        .count()\
        .orderBy(desc("count"))\
        .show()

    print('\nos 10 clientes que mais fizeram comentarios por videos\n')
    productsDF.join(reviewsDF,joinExpression)\
        .where("group = 'Video'")\
        .groupBy("customer_id")\
        .count()\
        .orderBy(desc("count"))\
        .show()



#getHelpfulReviews("0231118597")
#getTop10ProductsOfEachGroup()
#getTop10ProductsWithMostRatingAVG()
#getTop5CategoriesWithMostRating()
#get10CustomersWithMostReviewsForEachGroup()