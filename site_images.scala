package org.rubigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord

import org.jsoup.Jsoup
import org.jsoup.nodes.{Document,Element}
import collection.JavaConverters._
import org.apache.commons.lang3.StringUtils

object RUBigDataApp {
  def main(args: Array[String]) {
    // Specify your Redbad uname here:
    val uname = "s1008829"
    val warcfile = "s3://commoncrawl/crawl-data/CC-MAIN-2020-24/segments/1590347385193.5/warc/*warc.gz"

    val sparkConf = new SparkConf()
                      .setAppName("s3 test")
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .registerKryoClasses(Array(classOf[WarcRecord]))
    implicit val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = sparkSession.sparkContext
    
    val warcf = sc.newAPIHadoopFile(
              warcfile,
              classOf[WarcGzInputFormat],             // InputFormat
              classOf[NullWritable],                  // Key
              classOf[WarcWritable]                   // Value
    )

    val warc_filtered =                                      //__
     warcf.map{ wr => wr._2 }.                               //  |
        filter{ _.isValid() }.                               //  |  
        map{ _.getRecord() }.                                //  |
        filter{ _.hasContentHeaders() }.                     //  |
        filter{ _.isHttp() }.                                //  |
        filter{wr =>  wr.getHeader()                         //  |- Taking valid pages with interesting content
			.getHeaderValue("WARC-Type") == "response" }.    //  |
        filter{wr => wr.getHttpHeaders()                     //  |
			.get("Content-Type") != null}.                   //  |
        filter{wr => wr.getHttpHeaders()                     //  |
			.get("Content-Type").startsWith("text/html")}.   //__|
        map{wr => 
		(wr.getHeader().getUrl(), 
		StringUtils.normalizeSpace(wr.getHttpStringBody()))}.//   - Mapping these pages to simple (<URL>, <HTTP body>) pairs
        cache()                                              //   - Caching in case of multiple analyses on the same set
    
	val count = warc_filtered.count()
    
	val hostnamePattern = """^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)""".r
    
	def getHostname(text: String): Option[String] = for (m <- hostnamePattern.findFirstMatchIn(text)) yield m.group(1)

    val domain_imageCounts = warc_filtered.                                //__
            map{wr => (StringUtils.substring(                              //  |
                getHostname(wr._1).fold("")(_.toString),                   //  |- Key => Extracted top-level domain name
                getHostname(wr._1).fold("")(_.toString).lastIndexOf("."),  //  |
                getHostname(wr._1).fold("")(_.toString).length())          //__|
                , wr._2)}.                                                 //   - Value => HTTP content of the page
            flatMapValues{http =>                       
            Jsoup.parse(http).select("img[src]").asScala.        
            map{img => 1}}.                                 // Mapping values to "1" for each "img" tag in the HTML
            reduceByKey((a, b) => a + b).                   // Aggregating all the "1"s for all top-level domains
            sortBy(_._2, false).                            // Putting the domains with most images at the top
            cache()                                         // Caching for later use
            
    val domain_siteCounts = warc_filtered.                                           //__
                        map{wr => (StringUtils.substring(                            //  |         
                            getHostname(wr._1).fold("")(_.toString),                 //  |- Key => Extracted top-level domain name       
                            getHostname(wr._1).fold("")(_.toString).lastIndexOf("."),//  |             
                            getHostname(wr._1).fold("")(_.toString).length())        //__|        
                            , 1)}.                                                   //   - Value => "1"
                        reduceByKey((a, b) => a + b).  // Aggregating all the "1"s for all top-level domains
                        sortBy(_._2, false).           // Putting the domains with most pages at the top                            
                        cache()                        // Caching for later use

    val relative_imageCounts = domain_siteCounts.
                        filter(_._2 > 50).                       // Only take domains with more than 250 pages
                        join(domain_imageCounts).                 // Join the image count RDD to the page count one
                        mapValues{x =>
                        (x._2.toDouble / x._1.toDouble,           // First result value is the ratio images/sites
                        s"${x._2} images in ${x._1} websites")}.  // Second is the explanation of this ratio
                        sortBy(_._2._1, false).                   // Put the domains with most images per page on top
                        cache()                                   // Cache for repeated use

    val imageTop10 = domain_imageCounts.take(10)
    val siteTop10 =  domain_siteCounts.take(10)
    val relativeTop10 = relative_imageCounts.take(10)

    println("\n########## OUTPUT ##########")
    println(s"\nNumber of valid pages: $count\n")

    println("Domain image counts:")
    imageTop10.foreach{println}

    println("\nDomain site counts:")
    siteTop10.foreach{println}

    println("\n Relative image counts:")
    relativeTop10.foreach{println}    
    
    println("########### END ############\n")
    sc.stop()
  }
}
