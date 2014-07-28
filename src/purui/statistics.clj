(ns purui.statistics
  (:require [purui.io :as io]
            [clojure.java.io :as javaio]
            [clojure.string :as string]))

(defn easy-frequency
  [coll output-file cols]
  (let [coll (map #(select-keys % cols) coll)
        s (frequencies coll)
        c (map #(assoc (first %) :count (second %)) s)]
    (io/write-csv-quoted c output-file)))

(defn word-frequency
  [coll mid-file output-file cols]
  (let [mid-file2 (str mid-file 2)
        func (fn [p pivots]
               (let [s (group-by #(select-keys % pivots) p)]
                 (map #(assoc (first %) :count (reduce + (map read-string (map :count (second %))))) s)))]


    (println "First round accumulation begins, check file " mid-file)

    (let [coll (map #(select-keys % cols) coll)
          pars (partition-all 10000 coll)
          func (fn [p]
                 (let [s (frequencies p)]
                   (map #(assoc (first %) :count (second %)) s)))]
      (io/write-csv-quoted (func (first pars)) mid-file)
      (doseq [p (rest pars)]
        (io/write-csv-quoted (func p) mid-file :append true)))

    (println "Second round accumulation begins, check file " mid-file2)

    (let [coll (io/lazy-read-csv-head-on mid-file)
          pars (partition-all 200000 coll)
          pivots (keys (dissoc (first (io/lazy-read-csv-head-on mid-file)) :count))]
      (io/write-csv-quoted (func (first pars) pivots) mid-file2)
      (doseq [p (rest pars)]
        (io/write-csv-quoted (func p pivots) mid-file2 :append true)))
    ;(javaio/delete-file mid-file)

    (println "Results are to be distributed to every brand")

    (let [coll (io/lazy-read-csv-head-on mid-file2)
          a (atom {})]
      (println "Data are distributing to each brand...")
      (doseq [row coll]
        (io/write-csv-quoted-split-brand row a mid-file2))
      (doseq [w (vals @a)]
        (.close w))
      ;(javaio/delete-file mid-file2)

      (println "Last step. Output statistics for each brand")

      (doseq [f (keys @a)]
        (let [file (str mid-file2 "_" f ".csv")
              output-file (str output-file "_" f ".csv")
              coll (io/lazy-read-csv-head-on file)
              pivots (keys (dissoc (first (io/lazy-read-csv-head-on file)) :count))]
          (-> (func coll pivots)
              (#(sort (fn [x1 x2] (> (:count x1) (:count x2))) %))
              (io/write-csv-quoted output-file :encoding "GBK"))
          ;(javaio/delete-file file)
          )))

    (println "All jobs are done!")))

(defn word-frequency-to-csv
  [input-file mid-file output-file & cols]
  (word-frequency (io/lazy-read-csv-head-on input-file)
                  mid-file
                  output-file
                  cols))

(defn easy-frequency-to-csv
  [input-file output-file & cols]
  (easy-frequency (io/lazy-read-csv-head-on input-file)
                  output-file
                  cols))



;(word-frequency (io/lazy-read-csv-head-on "D:/data/segstext2.csv") "D:/data/mid-file.csv" "D:/data/news" :brand :publish_date :word :nature)


#_(word-frequency (io/lazy-read-csv-head-on "D:/data/segstext2.csv") "D:/data/hehe/mid-file.csv" "D:/data/hehe/news"
                :brand :word :nature)

;(word-frequency (io/lazy-read-csv-head-on "D:/data/segstext6.csv") "D:/data/mid-file.csv" "D:/data/weibo" :brand :publish_date :word :nature)

;(word-frequency (io/lazy-read-csv-head-on "D:/data/segstext6.csv") "D:/data/mid-file.csv" "D:/data/weibo" :brand :weibo_user_name :word :nature)

;(categorize (io/lazy-read-csv-head-on "D:/data/segstext6.csv") "D:/data/category.csv" "D:/data/weibo_category")

#_(word-frequency (io/lazy-read-csv-head-on "D:/data/weibo") "D:/data/mid-file.csv" "D:/data/weibo_category"
                :brand :publish_date :word :category)

;(easy-frequency (io/lazy-read-csv-head-on "D:/data/news_match.csv") "D:/data/news_regex.csv")
