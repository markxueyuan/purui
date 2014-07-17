(ns purui.io
  (:use clj-excel.core)
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure-csv.core :as clj-csv]
            [clojure.java.jdbc :as j]
            [clojure.string :as string]))

(defn lazy-read-excel-head-on
  [file]
  (let [coll (-> (lazy-workbook (workbook-hssf file))
                 first
                 second)
        head (map keyword (first coll))
        rows (rest coll)]
    (map #(zipmap head %) rows)))

(defn csv-to-map
  [csv-file]
  (let [in-file (io/reader csv-file)
        csv-seq (csv/read-csv in-file)
        lazy (fn lazy [wrapped]
               (lazy-seq
                (if-let [s (seq wrapped)]
                  (cons (first s) (lazy (rest s)))
                  (.close in-file))))
        coll (lazy csv-seq)]
    (into {} (rest coll))))

(defn lazy-read-csv
  [csv-file]
  (let [in-file (io/reader csv-file)
        csv-seq (csv/read-csv in-file)
        lazy (fn lazy [wrapped]
               (lazy-seq
                (if-let [s (seq wrapped)]
                  (cons (first s) (lazy (rest s)))
                  (.close in-file))))]
    (lazy csv-seq)))


(defn lazy-read-csv-head-on
  [file]
  (let [coll (lazy-read-csv file)
        head (map keyword (first coll))
        rows (rest coll)]
    (map #(zipmap head %) rows)))


(defn write-csv-quoted
  [coll file & {:keys [append encoding]}]
  (let [keys-vec (keys (first coll))
        vals-vecs (map (apply juxt keys-vec) coll)]
    (with-open [out (io/writer file :append append :encoding encoding)]
      (binding [*out* out]
        (when-not append
          (print (clj-csv/write-csv (vector (map name keys-vec)) :force-quote true)))
        (doseq [v vals-vecs]
          (let [v (map str v)]
            (print (clj-csv/write-csv (vector v) :force-quote true))))))))

(defn write-csv-quoted-by-row
  [row writer]
  (doto writer
    (.write (clj-csv/write-csv (vector (map str row)) :force-quote true))
    (.flush)))

(defn clean-line-break
  [string]
  (->> (string/split string #"[\r\n]")
       (string/join "   ")))


(defn write-csv-quoted-split-brand
  [row the-atom the-name & {:keys [encoding]}]
  (let [brand (:brand row)
        col (map name (keys row))
        value (map clean-line-break (vals row))
        writer (io/writer (str the-name "_" brand ".csv") :append true :encoding encoding)]
    (if-not (get @the-atom brand)
      (do
        (swap! the-atom assoc brand writer)
        (write-csv-quoted-by-row col writer)
        (write-csv-quoted-by-row value writer))
      (write-csv-quoted-by-row value (get @the-atom brand)))))


(defn lazy-read-mysql
  [& {:keys [host port db user password sql result-set-fn row-fn]
      :or {host "localhost"
           port "3306"
           db "test"
           user "root"
           password "othniel"
           sql "select * from test"
           result-set-fn (fn [rs] (doall (take 50 rs)))}}]
  (let [fetch-size Integer/MIN_VALUE
        db-spec {:classname "com.mysql.jdbc.Driver"
                 :subprotocol "mysql"
                 :subname (str "//" host ":" port "/" db)
                 :user user
                 :password password}
        cnxn (doto (j/get-connection db-spec)
               (.setAutoCommit false))
        stmt (j/prepare-statement cnxn sql
                                  :result-type :forward-only
                                  :concurrency :read-only
                                  :fetch-size fetch-size)
        _ (.setFetchSize stmt Integer/MIN_VALUE)]
    (if row-fn
      (j/query cnxn [stmt] :row-fn row-fn)
      (j/query cnxn [stmt] :result-set-fn result-set-fn))))

;(write-csv-quoted [{:a 2 :b "太差劲了你！"}] "D:/data/test_encoding2.csv" :encoding "GBK")

;(lazy-read-mysql :row-fn println :sql "select * from test limit 50")
