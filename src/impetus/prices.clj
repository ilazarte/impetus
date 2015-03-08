(ns impetus.prices
  (:require [clojure.string   :as str]
            [clojure.java.io  :as io]
            [clojure.data.csv :as csv]
            [impetus.time     :as time]))

(defn- times
  "Repeat a function n times against a value"
  [fx n val]
  (if (= n 0)
    val
    (recur fx (dec n) (fx val))))

(defn- indices
  "http://stackoverflow.com/questions/8641305/find-index-of-an-element-matching-a-predicate-in-clojure"
  [pred coll]
   (keep-indexed #(when (pred %2) %1) coll))

(defn- format-sw
  "basically format, but strip whitespace" 
  [fmt & args]
  (-> (apply format fmt args)
      (str/replace #"\s+" "")))

(defn- download
  "Download a url and save it as a file"
  [url file]
  (io/make-parents file)
  (with-open [i (io/input-stream url)
               o (io/output-stream file)]
    (io/copy i o)))

; default value in functions 
; variadic args: http://stackoverflow.com/questions/3208347/how-to-create-default-value-for-function-argument-in-clojure
; named values:  http://thinkrelevance.com/blog/2008/09/16/pcl-clojure-chapter-5
; named value seem to work best when you have a tuple of optional arguments
; otherwise, variadic functions seem really convenient and a little more clear
(defn- make-yahoo-historical-download-url
  ([symbol] (make-yahoo-historical-download-url symbol :year 1))
  ([symbol period length]
  "Downloads historical price for <symbol> of <period>
   Valid values for <period> are :day :month :year"
  (let [datetimes   (time/make-datetimes period length)
        start       (datetimes :start)
        end         (datetimes :end)
        start-month (start :month)
        start-day   (start :day)
        start-year  (start :year)
        end-month   (end   :month)
        end-day     (end   :day)
        end-year    (end   :year)]
		(format-sw "http://ichart.finance.yahoo.com/table.csv?
		      s=%s&
		      a=%s&b=%s&c=%s&
		      d=%s&e=%s&f=%s&
		      g=d&ignore=.csv" 
		      symbol
		      start-month start-day start-year
		      end-month end-day end-year)))) 

(defn- make-yahoo-intraday-download-url
  [symbol]
  (format-sw "http://chartapi.finance.yahoo.com/instrument/1.0/%s/chartdata;type=quote;range=5d/csv" symbol))

(defn- read-cached-yahoo-url
  "Read a url whose data will be cached for x minutes, makes filename unique to symbol"
  [url readfn maxtimeinminutes]
  (let [safeurl (str/replace (.substring url 7) #"[^\w]" "-")
        csvname (str "tmp/" safeurl ".cache")
        file    (io/as-file csvname)] 
    (if (.exists file)
      (let [maxtimeinmillis (* maxtimeinminutes 60 1000)
            modtime         (.lastModified file)
            currtime        (java.lang.System/currentTimeMillis)
            difftime        (- currtime modtime)
            update?         (> difftime maxtimeinmillis)]
        (when update?
          (download url file)))
      (download url file))
    (readfn file)))

(defn- make-yahoo-csv-line-parser
  "Higher order function to parse yahoo csv format
   Returns parsed down for options."
  []
  (let [conv  #(%1 %2)
        pdoub #(java.lang.Double/parseDouble %)
        plong #(java.lang.Long/parseLong %)
        cols  [:date :open :high :low :close :volume :adjclose]
        fns   [time/parse-date pdoub pdoub pdoub pdoub plong pdoub]]
    (fn
      [coll]
      (zipmap cols (map conv fns coll)))))

(defn- yahoo-historical-file-reader
  "Reads values from the csv, has to reverse since yahoo outputs most recent dates at top"
  [file]
  (with-open [in-file (io/reader file)]
    (let [lines (doall (csv/read-csv in-file))]
      (reverse (map (make-yahoo-csv-line-parser) (rest lines))))))

(defn- last-closes
  "get the last two closes from the intraday values"
  [prices]
  (let [vals  (drop 3 
                (map last 
                  (partition-by #(-> % :date .dayOfWeek .get) prices)))
        to-mid   #(.toDateTime (.toDateMidnight %))
        add-key  #(assoc % :adjclose (:close %))
        del-key  #(dissoc % :close)
        new-date #(update-in % [:date] to-mid)
        nvals    (map (comp new-date del-key add-key) vals)]
    nvals))

(defn- make-yahoo-intraday-parser
  "Produce a function to parse a valid csv line of the yahoo intraday format."
  []
  (let [conv  #(%1 %2)
        cols   [:date :close :high :low :open :volume]
        pdoub  #(java.lang.Double/parseDouble %)
        plong  #(java.lang.Long/parseLong %)
        pdate  #(-> (str % "000") time/parse-millis) 
        fns    [pdate pdoub pdoub pdoub pdoub plong]]
    (fn [line]
      (zipmap cols (map conv fns (re-seq #"[\d\.]+" line))))))

(defn intraday-prices
  "Download the intraday and ohlc price data"
  [symbol]
  (let [url    (make-yahoo-intraday-download-url symbol)
        reader #(slurp %)
        prices (read-cached-yahoo-url url reader 5)
        lines  (str/split prices #"\n")
        vol?   #(.startsWith (str/lower-case %) "volume")
        sidx   (-> 
                 (indices vol? lines)
                 first
                 inc)
        recs   (times rest sidx lines)
        parser (make-yahoo-intraday-parser)
        vals   (map parser recs)]
    vals))

(defn historical-prices
  "Download cache and ohlc price data
   Commenting out merging with last intraday as feed appears fixed
   TODO add symbol period length as another form"
  [symbol]
  (let [symbol-up    (str/upper-case symbol)
        url          (make-yahoo-historical-download-url symbol-up)
        time-in-mins (* 4 60)]
    (read-cached-yahoo-url url yahoo-historical-file-reader time-in-mins)))