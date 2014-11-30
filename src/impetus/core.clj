(ns impetus.core
  (:require [clojure.string  :as str]
            [impetus.prices  :as prices]
            [impetus.study   :as study]
            [impetus.time    :as time]))

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

; a lot of params are passed down
; this is a prime candidate for a map

(defn execute
  "Execute a study with a certain pricekey and param vector.
   Ideally we only do date operations once but processing time is fine.
   External error handling required" 
  [symbol study type pricekey params]
  (let [pricefn (if (= :historical type) prices/historical-prices prices/intraday-prices)
        prices  (pricefn symbol)
        symkey  (keyword (str/lower-case symbol))
        series  (map pricekey prices)
        dates   (map :date prices)
        values  (study/fractions->decimals (apply study series params))
        measure (mapv #(identity {:x (time/datetime->str %1) :y %2}) dates values)]
    {:symbol symbol
     :values measure}))

(defn relative
  "Not as sophisticated as original, but moving forward"
  [symbols study type pricekey & params]
  (let [fx       #(ex-as-nil (execute (name %) study type pricekey params))
        vals     (mapv fx symbols)
        not-nil? (complement nil?)]
    (filter not-nil? vals)))

; TODO not sure if this is still needed...
(defn relative-update
  "Designed to produce a current last value for a valid call to relative"
  [& args]
  (let [last-element (comp vector last)
        keep-last     #(update-in % [:values] last-element)]
    (mapv keep-last (apply relative args))))

(defn make-price-ma-ratio-list
  [symbols]
  (relative symbols study/price-to-moving-average-ratio :historical :adjclose 20))

(defn make-price-ma-ratio-list-intraday
  [symbols]
  (relative symbols study/price-to-moving-average-ratio :intraday :close 20))

(defn make-price-ma-ratio-list-update
  [symbols]
  (relative-update symbols study/price-to-moving-average-ratio :historical :adjclose 20))