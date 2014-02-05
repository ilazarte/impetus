(ns impetus.prices
  (:require [clojure.string   :as str]
            [clojure.java.io  :as io]
            [clojure.data.csv :as csv]
            [clj-time.core    :as ctime]
            [clj-time.format  :as ctime-format]))

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

;
; => (map #(ctime-format/unparse (% ctime-format/formatters) (ctime/now)) [:rfc822 :basic-week-date-time :t-time-no-ms :basic-date-time :basic-date-time-no-ms])
; ("Fri, 20 Sep 2013 05:30:18 +0000" "2013W385T053018.332Z" "T05:30:18Z" "20130920T053018.332Z" "20130920T053018Z")
;
(defn- explode-datetime
  [datetime]
  "Turns a date until a map of the date as well as its components formatted as strings.
   Returns month value is 0-indexed to be compatible with yahoo dates"
  (let [formatter (ctime-format/formatters :date)
        datestr   (ctime-format/unparse formatter datetime)
        datelist  (str/split datestr #"-")]
    {:datetime datetime
     :year     (nth datelist 0)
     :month    (as-> (nth datelist 1) x
                     (java.lang.Integer/valueOf x)
                     (- x 1)
                     (format "%02d" x)) 
     :day      (nth datelist 2)}))

(defn- make-datetimes
  ([] (make-datetimes :year 1))
  ([period length]
  "Get the days back and return start and end dates as a map
   Available periods are :day :week :month and :year"
  (let [end      (ctime/today-at-midnight)
        periodfn (cond (= period :day)   ctime/days
                       (= period :week)  ctime/weeks
                       (= period :month) ctime/months
                       (= period :year)  ctime/years
                       :else             ctime/days)
        start    (ctime/minus end (periodfn length))]
    {:start (explode-datetime start)
     :end   (explode-datetime end)})))

; default value in functions 
; variadic args: http://stackoverflow.com/questions/3208347/how-to-create-default-value-for-function-argument-in-clojure
; named values:  http://thinkrelevance.com/blog/2008/09/16/pcl-clojure-chapter-5
; named value seem to work best when you have a tuple of optional arguments
; otherwise, variadic functions seem really convenient and a little more clear
(defn- make-yahoo-download-url
  ([symbol] (make-yahoo-download-url symbol :year 1))
  ([symbol period length]
  "Downloads historical price for <symbol> of <period>
   Valid values for <period> are :day :month :year"
  (let [datetimes   (make-datetimes period length)
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

(defn- read-cached-yahoo-historical-url
  "Read a url whose data will be cached for x minutes, makes filename unique to symbol"
  [url readfn maxtimeinminutes]
  (let [symbol  (-> (re-seq #".*s=([A-Z]+).*" url) first last)
        csvname (str "tmp/" symbol ".cache")
        file    (io/as-file csvname)
        exists  (.exists file)]
    (when-not exists
      (download url file))
    (let [maxtimeinmillis (* maxtimeinminutes 60 1000)
          modtime         (.lastModified file)
          currtime        (java.lang.System/currentTimeMillis)
          difftime        (- currtime modtime)
          update?         (> difftime maxtimeinmillis)]
      (when update?
        (download url file))
      (readfn file))))

(defn- make-yahoo-csv-line-parser
  "Higher order function to parse yahoo csv format
   Returns parsed down for options."
  []
  (let [conv  #(%1 %2)
        fmt   (ctime-format/formatters :date)
        pdate #(ctime-format/parse fmt %)
        pdoub #(java.lang.Double/parseDouble %)
        plong #(java.lang.Long/parseLong %)
        cols  [:date :open :high :low :close :volume :adjclose]
        fns   [pdate pdoub pdoub pdoub pdoub plong pdoub]]
    (fn
      [coll]
      (zipmap cols (map conv fns coll)))))

(defn- yahoo-file-reader
  "Reads values from the csv, has to reverse since yahoo outputs most recent dates at top"
  [file]
  (with-open [in-file (io/reader file)]
    (let [lines (doall (csv/read-csv in-file))]
      (reverse (map (make-yahoo-csv-line-parser) (rest lines))))))

(defn historical-prices
  "Download cache and ohlc price data
   TODO add symbol period length as another form"
  [symbol]
  (let [symbol-up    (str/upper-case symbol)
        url          (make-yahoo-download-url symbol-up)
        time-in-mins (* 4 60)]
    (read-cached-yahoo-historical-url url yahoo-file-reader time-in-mins)))
