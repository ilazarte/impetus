(ns impetus.time
  (:require [clojure.string  :as str] 
            [clj-time.core   :as ctime]
            [clj-time.format :as cformat]))

(def ^:private edt (ctime/time-zone-for-offset -4))

;
; => (map #(ctime-format/unparse (% ctime-format/formatters) (ctime/now)) [:rfc822 :basic-week-date-time :t-time-no-ms :basic-date-time :basic-date-time-no-ms])
; ("Fri, 20 Sep 2013 05:30:18 +0000" "2013W385T053018.332Z" "T05:30:18Z" "20130920T053018.332Z" "20130920T053018Z")
;
(defn- explode-datetime
  [datetime]
  "Turns a date until a map of the date as well as its components formatted as strings.
   Returns month value is 0-indexed to be compatible with yahoo dates"
  (let [formatter (cformat/formatters :date)
        datestr   (cformat/unparse formatter datetime)
        datelist  (str/split datestr #"-")]
    {:datetime datetime
     :year     (nth datelist 0)
     :month    (as-> (nth datelist 1) x
                 (java.lang.Integer/valueOf x)
                 (- x 1)
                     (format "%02d" x)) 
     :day      (nth datelist 2)}))

(defn datetime->str
  "Convert a datetime to an rfc822 date"
  [datetime]
  (let [fmt (:rfc822 cformat/formatters)]
    (cformat/unparse fmt datetime)))

; TODO expand to multiple parser explained in clj-time docs
(defn parse-date [str]
  "parse a standard date into eastern daylight time of the format yyyy-MM-dd"
  (let [fmt (cformat/with-zone (cformat/formatters :date) edt)]
    (cformat/parse fmt str)))

(defn make-datetimes
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
