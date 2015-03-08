(ns impetus.study)

;--------------------------------------------------------------
;  Operators: functions that operate more like R or thinkscript.
;--------------------------------------------------------------

(defn- resize-forms
  "Given a collection of forms, 
   make all scalars a repeat of the same size as the largest collection
   Input must be a valid collection, not simply a scalar.

   TODO should this also pad nils for short collections?"
  [forms]
  (let [counter  #(if (coll? %) (count %) -1)
        max      (apply max (map counter forms))
        nomax    (= max -1)
        repeater #(if (coll? %) 
                    % 
                    (if nomax % (repeat max %)))]
    (map repeater forms)))

(defn augment
  "Wraps a function for use in the study system.

   1. Nil values become a short circuit so nil is returned if any arg is nil.
   2. Parameters can be collections or scalars
   3. More than two parameters are allowed
   4. If any arg is a collection, the args are all treated as collections.

   These new features allow you to return natural collections for all operations.
   It eliminates the need for nil checking so leading uncalculatable values are ignored.

   Example: 
     (def mul (augment *))
     => (mul 5 (range 2))
     (0 5)"
  [fx]
  (let [coerce #(if (nil? %) nil %)
        nilfn  (fn [& args]
                 (let [argscnt (count args)
                       ismulti (> argscnt 1)
                       argsnil (some nil? args)
                       args0   (first args)
                       iscoll  (if-not ismulti (coll? args0) nil)
                       colnil  (if iscoll (some nil? args0) nil)]
                   (cond
                     argsnil nil
                     ismulti (apply fx args)
                     iscoll  (apply fx args0)
                     :else   args)))
        callfn (fn 
                 [& args]
                 (let [argscnt    (count args)
                       ismulti    (> argscnt 1)
                       hascoll    (some coll? args)
                       ismulwcoll (and ismulti hascoll)
                       reargs     (resize-forms args)]
                   (cond 
                     ismulwcoll (apply map nilfn reargs)
                     ismulti    (apply nilfn reargs)
                     :else      (nilfn (first args)))))]
    callfn))

(def mul (augment *))
(def div (augment /))
(def add (augment +))
(def sub (augment -))
(def lt (augment <))
(def gt (augment >))

(defn window
  "Apply a function on a window into a collection
  TODO wow, this could have been simpler...
       http://stackoverflow.com/questions/1427894/sliding-window-over-seq
  Usage: 
    Moving average of 20 on a 100 numbers, no extra parameters.
    (window (range 100) 20 average)" 
  [coll len fx & params]
  (let [veccoll (vec coll)
        maxidx  (inc (count veccoll))]
    (loop [i   1
           res (transient [])]
      (cond
        (< i len) (recur 
                    (inc i) 
                    (conj! res nil))
        (< i maxidx) (let [start (- i len)
                           svec  (subvec veccoll start i)
                           val   (apply fx svec params)]
                       (recur 
                         (inc i) 
                         (conj! res val)))
        :else (persistent! res)))))

(defn shift
  "Move a collection values by n position.
   Adds or removes values as necessary
   
   Usage:
    => (shift (range 5) 1)
    (nil 0 1 2 3)"
  [coll n]
  (let [cnt  (count coll)
        pos  (> 0 n)
        absn (Math/abs n)
        rem  (- cnt absn)
        nils (repeat absn nil)]
    (if pos
      (concat (drop absn coll) nils)
      (concat nils (take rem coll)))))

;--------------------------------------------------------------
; Helper functions for use in studies
;--------------------------------------------------------------

(defn average
  "A simple average of the values of a collection"
  [coll]
  (div (add coll) (count coll)))

(defn round 
  "Set the scale on the big decimal"
  [bd scale] 
  (.setScale bd scale java.math.RoundingMode/HALF_EVEN)) 

(defn fractions->decimals
  "Convert a collection potentially of nils and fractions into decimals to two places.
   Other possibilities, float, double
   TODO understand with-precision and scale interchange"
  [coll]
  (map #(if (nil? %) nil (round (with-precision 4 (bigdec %)) 4)) coll))

(defn plot
  "Sketch of what it mike look like to have a plot function like thinkscript."
  [coll kw vals]
  (map #(assoc %1 kw %2) coll vals))

;-----------------------------------------------------------------
; Study
;   
;    All should take a collection of prices and return a new collection of the same size
;    Note that all studies could potentially return a fraction
;    Convenience function decimals converts to final representation
;
;    In final implementation goal should be to pass in ohlc and volume data.
;    For now, adjusted close, date, volume will have to do for historical prices.
;    For live prices, we should be able to get complete ohlc.
;-----------------------------------------------------------------

(defn simple-moving-average
  "Create a simple moving average of the values"
  [coll len]
  (window coll len average))

(defn percent-change
  "Percent change between period"
  [coll]
  (let [old (shift coll 1)
        chg (sub coll old)]
    (div chg old)))

(defn moving-average-ratio
  "Divide one moving average by another"
  [coll alen blen]
  (let [a (simple-moving-average coll alen)
        b (simple-moving-average coll blen)]
    (div a b)))

(defn price-to-moving-average-ratio
  "Divide a value by its moving average"
  [coll len]
  (let [ma (simple-moving-average coll len)]
    (div coll ma)))