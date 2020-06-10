# CC 2020

Final assignment 2020 (CommonCrawl).

    docker pull rubigdata/project:v0.9.1
    docker create --name cc -p 9001:8080 -p 4040-4045:4040-4045 rubigdata/project:v0.9.1
    docker start cc

Open [`localhost:9001`](localhost:9001) in your browser, and import the WARC for Spark notebook.
