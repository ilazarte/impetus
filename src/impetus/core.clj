(ns impetus.core
  (:require [clojure.string  :as str]
            [clj-time.core   :as ctime]
            [clj-time.format :as cformat]
            [impetus.prices  :as prices]
            [impetus.study   :as study]))

(def ^:private main-watchlist 
  ["AAPL", "AMZN", "BAC", "BIDU", "CAT",
   "DIA", "EEM", "EWZ", "FCX", "FFIV", 
   "FSLR", "GLD", "GOOG", "GS", "IBM",
   "IBM", "IWM", "JPM", "KO", "MCD",
   "NFLX", "PG", "PM", "QCOM", "QQQ",
   "SLV", "SPY", "USO", "WMT", "XOM"])

(def ^:private index-watchlist 
  ["XLK","XLF","XLP","XLE","XLI",
   "XLV","XLY","XLU","XLB","GLD",
   "SLV","SPY","IWM","DIA","EWZ",
   "USO","EEM","QQQ","TLT"])

(def ^:private test-watchlist 
  ["GOOG", "AAPL", "IBM", "SPY", "RIMM"])

(defmacro ^:private ex-as-nil
  [forms]
  `(try
     ~forms
     (catch Exception e# nil)))

(defn- datetime->str
  [datetime]
  "Convert a datetime to an rfc822 date"
  (let [fmt (:rfc822 cformat/formatters)]
    (cformat/unparse fmt datetime)))

(defn- formatlocal [n offset]
  "format a date for use by javascript" 
  (let [nlocal (ctime/to-time-zone n (ctime/time-zone-for-offset offset))
        fmt    (cformat/formatters :date-time-no-ms)]
    (cformat/unparse fmt nlocal)))

(defn- format-edt [n]
  "format a date for eastern daylight time"
  (formatlocal n -4))

(defn execute
  "Execute a study with a certain pricekey and param vector.
   Ideally we only do date operations once but processing time is fine.
   External error handling required" 
  [symbol study pricekey params]
  (let [prices  (prices/historical-prices symbol)
        symkey  (keyword (str/lower-case symbol))
        series  (map pricekey prices)
        dates   (map :date prices)
        values  (study/fractions->decimals (apply study series params))
        measure (mapv #(identity {:x (format-edt %1) :y %2}) dates values)]
    {:symbol symbol
     :values measure}))

(defn relative
  "Not as sophisticated as original, but moving forward"
  [symbols study pricekey & params]
  (let [fx   #(ex-as-nil (execute (name %) study pricekey params))
        vals (mapv fx symbols)]
    (filter (complement nil?) vals)))

(defn relative-update
  "Designed to produce a current last value for a valid call to relative"
  [& args]
  (let [last-element (comp vector last)
        keep-last     #(update-in % [:values] last-element)]
    (mapv keep-last (apply relative args))))

(defn make-price-ma-ratio-list
  [symbols]
  (relative symbols study/price-to-moving-average-ratio :adjclose 20))

(defn make-price-ma-ratio-list-update
  [symbols]
  (relative-update symbols study/price-to-moving-average-ratio :adjclose 20))