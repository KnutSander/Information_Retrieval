#!usr/bin/env python3

"""Does all the operations specified in the assignment, in Python"""

#-------------------------------------------------------------------------------
# Imports and Global Variables
#-------------------------------------------------------------------------------

# Import Elasticsearch
import json, time
from elasticsearch import Elasticsearch

# Index and document type
index = "movies"
doc_type = "plots"

# Define custom analysers, similarity module for TF.IDF scores and define the mapping
body = {
  "settings": {
    "number_of_shards": 1, 
    "analysis": {
      "analyzer": {
        "token_and_fold": {
          "type": "custom",
          "tokenizer": "lowercase"
        },
        "ngram_and_stop": {
          "type": "custom",
          "tokenizer": "lowercase",
          "filter": [
            "stop",
            "ngram"
          ]
        },
        "stem": {
          "type": "custom",
          "tokenizer": "lowercase",
          "filter": [
            "stemmer"  
          ]
        }
      }
    },
    "similarity": {
      "tf_idf": {
        "type": "scripted",
        "script": {
          "source": "double tf = Math.sqrt(doc.freq); double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0; double norm = 1/Math.sqrt(doc.length); return query.boost * tf * idf * norm;"
        }
      }
    }
  },
  "mappings": {
    "plots":{
      "properties": {
        "Plot": {
          "type": "text",
          "similarity": "tf_idf",
          "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
        }
      }
    }
  }
}


# Assign the TF.IDF similarity module to the "Plot"
mapping = {
    "properties": {
        "Plot": {
            "type": "text",
            "similarity": "tf_idf"
        }
    }
}


# Analyser testing
anal1 = {
    "analyzer":"token_and_fold",
    "text": "Big SALTy ChocolaTE BaLLs"
}

anal2 ={
    "analyzer": "ngram_and_stop",
    "text": "Was that him?"
}

anal3 = {
    "analyzer": "stem",
    "text": "Big SALTy ChocolaTE BaLLs"
}


# Index searching
search1 = {
  "query": {
    "match": {
      "Genre": "western"
    }
  }
}

search2 = {
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "Genre": "horror"
          }
        }
      ],
      "should": [
        {
          "match": {
            "Origin/Ethnicity": "American"
          }
        }
      ]
    }
  },
  "sort": [
    {
      "Release Year": {
        "order": "desc"
      }
    }
  ]
}

search3 = {
  "query": {
    "bool": {
      "must": {
        "match": {
          "Plot": "drama"
        }
      },
      "filter": {
        "range": {
          "Release Year": {
            "lt": 1920
          }
        }
      }
    }
  }
}

#-------------------------------------------------------------------------------
# Routines
#-------------------------------------------------------------------------------

# Small function to print the results
def printResult(res, res_type):
    if res_type:
        for token in res['tokens']:
            print(token)
    else:
        print(res['hits'])
        for hit in res['hits']['hits']:
            print(hit)


#-------------------------------------------------------------------------------
# Main Program
#-------------------------------------------------------------------------------

# Before running the program, start Elasticsearch in console

# Try to establish a connection to the locally hosted Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Make sure a connection has been established
if not es.ping():
    raise ValueError("Connection to Elasticsearch failed")
    exit(1)
else:
    print("Connection to Elasticsearch successful")

# Make sure to reset the index so settings can be imported
es.indices.delete(index)

# Import the settings
es.indices.create(index, body)

# Import the json file data
print("Importing data")
with open('first_1000_wiki_movie_plots_deduped.json', 'r') as json_file:
    data = json_file.read()

print("Sending data to Elasticsearch")
# Load the document into Elasticsearch
es.bulk(data, index, doc_type)
print("Data upload successful")


# Test tokenisation and folding
print("Tokenisation and Case Folding\n" + 
      "Sample input: 'Big SALTy ChocolaTE BaLLs'")
res = es.indices.analyze(body=anal1, index="movies")
printResult(res, True)
input("Press Enter for next test\n")

# Stop-word removal and ngram
print("Stop-Word Removal and N-Gram Tokeniser\n" +
      "Sample input: 'Was that him?'")
res = es.indices.analyze(body=anal2, index="movies")
printResult(res, True)
input("Press Enter for next test\n")

# Stemming
print("Stemming\n" + "Sample input: 'Big SALTy ChocolaTE BaLLs'")
res = es.indices.analyze(body=anal3, index="movies")
printResult(res, True)
input("Press Enter to move on to searches\n")

# Western movies
print("Western movies")
res = es.search(body=search1, index="movies")
printResult(res, False)
input("Press Enter for next search\n")

# Horror movies, should be American
print("Horror movies, should be American")
res = es.search(body=search2, index="movies")
printResult(res, False)
input("Press Enter for next search\n")

# Movies where the plot mentions drama, from before 1920
print("Movies that mention drama, made before 1920")
res = es.search(body=search3, index="movies")
printResult(res, False)

# END
